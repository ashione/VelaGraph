use anyhow::{bail, ensure, Context, Result};
use replicated_graph::{
    catalog::{Catalog, Placement},
    remote::{
        self,
        query::{QueryMode, QueryRequest},
    },
    routing::{experimental::*, RemoteReplica, RouteResolver, ScanScope, ShardReadStamp},
    run_blocking,
};
use serde::Deserialize;
use serde_json::{json, Value as Json};
use std::{collections::BTreeMap, io::Read, net::SocketAddr, path::Path, time::Duration};
use velagraph::{
    experimental_graph::{CrossEdge, VertexRef},
    GlobalScanOutput, GremlinRow, PathElement, Value, Vertex,
};

const MAX_INPUT: u64 = 1024 * 1024;
const HELP: &str = "graph-client --config FILE --allow-insecure-development COMMAND\nconfig-check\ncatalog lookup --partition N\ncatalog register --partition N --file PLACEMENT.json\nv1 query --partition N --query GREMLIN\nv1 global-query --query GREMLIN\nv1 server-query --group N --node N --shard N --partition N --query GREMLIN\nexperimental server-query --group N --node N --shard N --partition N --query GREMLIN\nexperimental vertex create --partition N --file VERTEX.json\nexperimental edge create --from PARTITION:ID --to PARTITION:ID --id N --label LABEL\nexperimental observed-query --start-partition N --query GREMLIN\nAll stdout is versioned JSON. Plaintext trusted development networks only. Writes are single attempts, not durable retry sessions.";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlacementConfig {
    shard: u64,
    group: u64,
    epoch: u64,
    replicas: Vec<u64>,
}
impl PlacementConfig {
    fn placement(&self) -> Result<Placement> {
        ensure!(self.epoch > 0, "epoch must be positive");
        ensure!(
            !self.replicas.is_empty()
                && self.replicas.len() <= 31
                && self.replicas.windows(2).all(|w| w[0] < w[1]),
            "replicas must be sorted, unique, nonempty and at most 31"
        );
        Ok(Placement {
            shard: self.shard,
            group: self.group,
            epoch: self.epoch,
            replicas: self.replicas.clone(),
        })
    }
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicaConfig {
    group: u64,
    node: u64,
    endpoint: SocketAddr,
}
impl ReplicaConfig {
    fn replica(&self) -> Result<RemoteReplica> {
        ensure!(
            self.endpoint.port() != 0 && !self.endpoint.ip().is_unspecified(),
            "endpoint must have a concrete address and nonzero port"
        );
        Ok(RemoteReplica {
            group: self.group,
            node: self.node,
            endpoint: self.endpoint,
        })
    }
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    version: u32,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    metadata: PlacementConfig,
    metadata_node: u64,
    metadata_nodes: Vec<ReplicaConfig>,
    data_nodes: Vec<ReplicaConfig>,
}
fn default_timeout() -> u64 {
    10_000
}
struct Client {
    config: Config,
    resolver: RouteResolver,
    catalog: Catalog,
    timeout: Duration,
}
impl Config {
    fn client(self) -> Result<Client> {
        ensure!(self.version == 1, "unsupported configuration version");
        ensure!(
            (1..=60_000).contains(&self.timeout_ms),
            "timeout_ms must be 1..=60000"
        );
        let timeout = Duration::from_millis(self.timeout_ms);
        let placement = self.metadata.placement()?;
        let metadata = self
            .metadata_nodes
            .iter()
            .map(ReplicaConfig::replica)
            .collect::<Result<Vec<_>>>()?;
        let data = self
            .data_nodes
            .iter()
            .map(ReplicaConfig::replica)
            .collect::<Result<Vec<_>>>()?;
        let mut actual = metadata.iter().map(|r| r.node).collect::<Vec<_>>();
        actual.sort_unstable();
        ensure!(
            actual == placement.replicas,
            "metadata_nodes must exactly match metadata replicas"
        );
        let selected = metadata
            .iter()
            .find(|r| r.node == self.metadata_node)
            .context("metadata_node is not configured")?;
        let resolver = RouteResolver::new_remote(placement.clone(), &metadata, &data)?;
        let catalog = Catalog::new_remote(selected.endpoint, selected.node, placement, timeout)?;
        Ok(Client {
            config: self,
            resolver,
            catalog,
            timeout,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistrationFile {
    version: u32,
    shard: u64,
    group: u64,
    epoch: u64,
    replicas: Vec<u64>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VertexFile {
    version: u32,
    id: u64,
    labels: Vec<String>,
    #[serde(deserialize_with = "unique_properties")]
    properties: BTreeMap<String, Value>,
}
fn unique_properties<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, Value>, D::Error> {
    struct Properties;
    impl<'de> serde::de::Visitor<'de> for Properties {
        type Value = BTreeMap<String, Value>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("unique property names and tagged scalar values")
        }
        fn visit_map<M: serde::de::MapAccess<'de>>(
            self,
            mut map: M,
        ) -> std::result::Result<Self::Value, M::Error> {
            let mut result = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, Value>()? {
                if result.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate property name"));
                }
            }
            Ok(result)
        }
    }
    deserializer.deserialize_map(Properties)
}

fn read_json<T: serde::de::DeserializeOwned>(path: &str) -> Result<T> {
    ensure!(
        std::fs::metadata(path)
            .context("inspect JSON input")?
            .is_file(),
        "JSON input must be a regular file"
    );
    let file = std::fs::File::open(Path::new(path)).context("open JSON input")?;
    ensure!(
        file.metadata()?.is_file(),
        "JSON input must be a regular file"
    );
    let mut bytes = Vec::new();
    file.take(MAX_INPUT + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= MAX_INPUT, "JSON input exceeds 1 MiB");
    serde_json::from_slice(&bytes).context("invalid JSON input")
}

#[derive(Debug)]
enum Command {
    Check,
    Lookup(u32),
    Register(u32, Placement),
    Query(u32, String),
    ServerQuery {
        group: u64,
        node: u64,
        shard: u64,
        partition: u32,
        mode: QueryMode,
        text: String,
    },
    Global(String),
    Observed(u32, String),
    Vertex(u32, Vertex),
    Edge(CrossEdge),
}
fn number<T: std::str::FromStr>(text: &str) -> Result<T> {
    ensure!(
        !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()),
        "expected unsigned decimal integer"
    );
    text.parse()
        .map_err(|_| anyhow::anyhow!("integer out of range"))
}
fn vertex_ref(text: &str) -> Result<VertexRef> {
    let (partition, id) = text
        .split_once(':')
        .context("vertex reference must be PARTITION:ID")?;
    Ok(VertexRef {
        partition: number(partition)?,
        id: number(id)?,
    })
}
fn take(options: &mut BTreeMap<String, String>, name: &str) -> Result<String> {
    options
        .remove(name)
        .with_context(|| format!("{name} is required"))
}
fn parse_command(words: &[String], mut opts: BTreeMap<String, String>) -> Result<Command> {
    let words = words.iter().map(String::as_str).collect::<Vec<_>>();
    let result = match words.as_slice() {
        ["config-check"] => Command::Check,
        ["catalog", "lookup"] => Command::Lookup(number(&take(&mut opts, "--partition")?)?),
        ["catalog", "register"] => {
            let partition = number(&take(&mut opts, "--partition")?)?;
            let file: RegistrationFile = read_json(&take(&mut opts, "--file")?)?;
            ensure!(file.version == 1, "unsupported placement version");
            let placement = PlacementConfig {
                shard: file.shard,
                group: file.group,
                epoch: file.epoch,
                replicas: file.replicas,
            }
            .placement()?;
            Command::Register(partition, placement)
        }
        ["v1", "query"] => Command::Query(
            number(&take(&mut opts, "--partition")?)?,
            take(&mut opts, "--query")?,
        ),
        [namespace @ ("v1" | "experimental"), "server-query"] => Command::ServerQuery {
            group: number(&take(&mut opts, "--group")?)?,
            node: number(&take(&mut opts, "--node")?)?,
            shard: number(&take(&mut opts, "--shard")?)?,
            partition: number(&take(&mut opts, "--partition")?)?,
            mode: if *namespace == "v1" {
                QueryMode::V1
            } else {
                QueryMode::V2Observed
            },
            text: take(&mut opts, "--query")?,
        },
        ["v1", "global-query"] => Command::Global(take(&mut opts, "--query")?),
        ["experimental", "observed-query"] => Command::Observed(
            number(&take(&mut opts, "--start-partition")?)?,
            take(&mut opts, "--query")?,
        ),
        ["experimental", "vertex", "create"] => {
            let partition = number(&take(&mut opts, "--partition")?)?;
            let file: VertexFile = read_json(&take(&mut opts, "--file")?)?;
            ensure!(file.version == 1, "unsupported vertex version");
            Command::Vertex(
                partition,
                Vertex {
                    id: file.id,
                    labels: file.labels,
                    properties: file.properties,
                },
            )
        }
        ["experimental", "edge", "create"] => Command::Edge(CrossEdge {
            from: vertex_ref(&take(&mut opts, "--from")?)?,
            to: vertex_ref(&take(&mut opts, "--to")?)?,
            id: number(&take(&mut opts, "--id")?)?,
            label: take(&mut opts, "--label")?,
            properties: BTreeMap::new(),
        }),
        _ => bail!("unknown command; use --help"),
    };
    ensure!(opts.is_empty(), "unknown or unused command option");
    Ok(result)
}

pub async fn run(args: Vec<String>) -> Result<Json> {
    if args == ["--help"] || args == ["-h"] {
        return Ok(json!({"version":1,"ok":true,"help":HELP}));
    }
    ensure!(
        args.iter().map(String::len).sum::<usize>() <= MAX_INPUT as usize,
        "arguments exceed 1 MiB"
    );
    let mut opts = BTreeMap::new();
    let mut words = Vec::new();
    let mut trusted = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--allow-insecure-development" {
            ensure!(!trusted, "duplicate --allow-insecure-development");
            trusted = true;
        } else if arg.starts_with("--") {
            let value = args.next().context("option requires a value")?;
            ensure!(opts.insert(arg, value).is_none(), "duplicate option");
        } else {
            words.push(arg);
        }
    }
    ensure!(trusted, "plaintext clients require --allow-insecure-development; use only a trusted isolated network");
    let config: Config = read_json(&take(&mut opts, "--config")?)?;
    let command = parse_command(&words, opts)?;
    let client = config.client()?;
    if let Command::ServerQuery {
        group,
        node,
        shard,
        partition,
        mode,
        text,
    } = command
    {
        let endpoint = client
            .config
            .data_nodes
            .iter()
            .find(|r| r.group == group && r.node == node)
            .context("explicit query replica is not in data_nodes")?
            .endpoint;
        let output = remote::query::query(
            endpoint,
            Some(remote::RemoteIdentity { group, node }),
            QueryRequest {
                shard,
                partition,
                mode,
                text,
                timeout_millis: client.config.timeout_ms.min(5000),
            },
            client.timeout,
        )
        .await??;
        return Ok(
            json!({"version":1,"ok":true,"execution":"server","scope":"single_partition","output":output}),
        );
    }
    run_blocking(move || execute(client, command)).await?
}

fn placement_json(p: &Placement) -> Json {
    json!({"group":p.group,"shard":p.shard,"epoch":p.epoch,"replicas":p.replicas})
}
fn stamps(stamps: &[ShardReadStamp]) -> Vec<Json> {
    stamps
        .iter()
        .map(|s| json!({"group":s.group,"shard":s.shard,"epoch":s.epoch,"sequence":s.sequence}))
        .collect()
}
fn element(kind: &str, partition: u32, id: u64) -> Json {
    json!({"type":kind,"partition":partition,"id":id})
}
fn v1_row(partition: u32, row: GremlinRow) -> Json {
    match row {
        GremlinRow::Vertex(id) => element("vertex", partition, id),
        GremlinRow::Edge(id) => element("edge", partition, id),
        GremlinRow::Count(n) => json!({"type":"count","value":n}),
        GremlinRow::Value(value) => json!({"type":"value","partition":partition,"value":value}),
        GremlinRow::Path(path) => {
            json!({"type":"path","elements":path.into_iter().map(|p| match p { PathElement::Vertex(id) => element("vertex",partition,id), PathElement::Edge(id) => element("edge",partition,id) }).collect::<Vec<_>>() })
        }
    }
}
fn observed_row(row: ObservedRowV3) -> Json {
    match row {
        ObservedRowV3::Vertex(v) => element("vertex", v.partition, v.id),
        ObservedRowV3::Edge(e) => element("edge", e.partition, e.id),
        ObservedRowV3::Count(n) => json!({"type":"count","value":n}),
        ObservedRowV3::PropertyMap { source, properties } => json!({
            "type": "property_map", "source": element("vertex", source.partition, source.id),
            "properties": properties,
        }),
        ObservedRowV3::Value { source, value } => {
            let source = match source {
                ObservedElementRef::Vertex(v) => element("vertex", v.partition, v.id),
                ObservedElementRef::Edge(e) => element("edge", e.partition, e.id),
            };
            json!({"type":"value","source":source,"value":value})
        }
        ObservedRowV3::Path(path) => {
            json!({"type":"path","elements":path.into_iter().map(|p| match p { ObservedPathElement::Vertex(v) => element("vertex",v.partition,v.id), ObservedPathElement::Edge(e) => element("edge",e.partition,e.id) }).collect::<Vec<_>>() })
        }
    }
}
fn observed_json(result: ObservedTraversalV3) -> Json {
    json!({"version":1,"ok":true,"namespace":"experimental-v2","completeness":format!("{:?}",result.completeness),"metadata_sequence":result.metadata_sequence,"snapshots":stamps(&result.snapshots),"rows":result.rows.into_iter().map(observed_row).collect::<Vec<_>>()})
}

fn request_id(id: velagraph::RequestId) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Debug)]
struct WriteFailure {
    details: Json,
}
impl std::fmt::Display for WriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("write did not receive a complete acknowledgement; inspect JSON outcome")
    }
}
impl std::error::Error for WriteFailure {}
fn append(client: &Client, prepared: PreparedAppend, identity: Json) -> Result<Json> {
    let source = request_id(prepared.source_request_id());
    let reverse = prepared.reverse_request_id().map(request_id);
    let acknowledgement = client
        .resolver
        .experimental_commit(&prepared, client.timeout)
        .map_err(|error| append_failure(&error, &source, &reverse, &identity))?;
    Ok(
        json!({"version":1,"ok":true,"namespace":"experimental-v2","acknowledgement":format!("{acknowledgement:?}"),"identity":identity,"source_request_id":source,"reverse_request_id":reverse,"atomic":false,"retry_supported":false}),
    )
}

fn append_failure(
    error: &ExperimentalRouteError,
    source: &str,
    reverse: &Option<String>,
    identity: &Json,
) -> WriteFailure {
    let mut details = json!({"kind":"experimental_write","source_request_id":source,"reverse_request_id":reverse,"identity":identity,"retry_supported":false,"error":format!("{error}")});
    if let ExperimentalRouteError::Stage {
        stage,
        outcome,
        request_id: id,
        source_acknowledged_this_attempt,
        ..
    } = error
    {
        details["stage"] = json!(format!("{stage:?}"));
        details["outcome"] = json!(format!("{outcome:?}"));
        details["request_id"] = json!(request_id(*id));
        details["source_acknowledged_this_attempt"] = json!(source_acknowledged_this_attempt);
    }
    WriteFailure { details }
}

fn execute(client: Client, command: Command) -> Result<Json> {
    match command {
        Command::ServerQuery { .. } => bail!("server query requires asynchronous dispatch"),
        Command::Check => {
            Ok(json!({"version":1,"ok":true,"namespace":"configuration","network_contacted":false}))
        }
        Command::Lookup(partition) => {
            let snapshot = client.catalog.snapshot()?;
            let placement = snapshot
                .placement(partition)
                .context("partition is not registered")?;
            Ok(
                json!({"version":1,"ok":true,"namespace":"catalog","partition":partition,"placement":placement_json(placement),"metadata_sequence":snapshot.sequence()}),
            )
        }
        Command::Register(partition, placement) => {
            ensure!(
                placement.group != client.config.metadata.group
                    && placement.shard != client.config.metadata.shard,
                "metadata group/shard are reserved"
            );
            for node in &placement.replicas {
                ensure!(
                    client
                        .config
                        .data_nodes
                        .iter()
                        .any(|r| r.group == placement.group && r.node == *node),
                    "placement replica is not configured"
                );
            }
            let mut id = [0u8; 16];
            getrandom::fill(&mut id)
                .map_err(|_| anyhow::anyhow!("request identity generation failed"))?;
            let prepared = client
                .catalog
                .prepare_registration(id, partition, placement.clone())?;
            let result = client.catalog.commit_registration(&prepared).map_err(|error| WriteFailure { details: json!({"kind":"catalog_write","request_id":request_id(id),"expected_sequence":prepared.expected_sequence(),"outcome":"Unknown","retry_supported":false,"error":format!("{error}")}) })?;
            Ok(
                json!({"version":1,"ok":true,"namespace":"catalog","partition":partition,"placement":placement_json(&placement),"request_id":request_id(id),"sequence":result.sequence,"status":format!("{:?}",result.status),"retry_supported":false}),
            )
        }
        Command::Query(partition, query) => {
            let view = client.resolver.read_view(partition, client.timeout)?;
            let result = view.execute_gremlin(&query)?;
            let p = view.placement();
            Ok(
                json!({"version":1,"ok":true,"namespace":"v1","metadata_sequence":view.metadata_sequence(),"snapshots":[{"group":p.group,"shard":p.shard,"epoch":view.epoch(),"sequence":view.sequence()}],"rows":result.rows.into_iter().map(|r| v1_row(partition,r)).collect::<Vec<_>>()}),
            )
        }
        Command::Global(query) => {
            let result =
                client
                    .resolver
                    .execute_global_scan(ScanScope::All, &query, client.timeout)?;
            let rows = match result.result {
                GlobalScanOutput::Rows(rows) => rows
                    .into_iter()
                    .map(|r| v1_row(r.partition, r.row))
                    .collect(),
                GlobalScanOutput::Count(n) => vec![json!({"type":"count","value":n})],
            };
            Ok(
                json!({"version":1,"ok":true,"namespace":"v1","metadata_sequence":result.metadata_sequence,"snapshots":stamps(&result.snapshots),"rows":rows}),
            )
        }
        Command::Observed(partition, query) => {
            let session = client.resolver.experimental_read_session(client.timeout)?;
            let result = session.observed_gremlin_v3(partition, &query)?;
            Ok(observed_json(result))
        }
        Command::Vertex(partition, vertex) => {
            let prepared = client
                .resolver
                .experimental_read_session(client.timeout)?
                .prepare_vertex(partition, &vertex)?;
            append(&client, prepared, element("vertex", partition, vertex.id))
        }
        Command::Edge(edge) => {
            let prepared = client
                .resolver
                .experimental_read_session(client.timeout)?
                .prepare_edge(&edge)?;
            append(
                &client,
                prepared,
                element("edge", edge.from.partition, edge.id),
            )
        }
    }
}
pub fn error_json(error: &anyhow::Error) -> Json {
    let details = error
        .downcast_ref::<WriteFailure>()
        .map(|e| e.details.clone())
        .unwrap_or_else(|| json!({"kind":"command_error","message":format!("{error:#}")}));
    json!({"version":1,"ok":false,"error":details})
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn config() -> Json {
        json!({"version":1,"timeout_ms":100,"metadata":{"group":1,"shard":101,"epoch":1,"replicas":[1]},"metadata_node":1,"metadata_nodes":[{"group":1,"node":1,"endpoint":"127.0.0.1:10101"}],"data_nodes":[{"group":2,"node":1,"endpoint":"127.0.0.1:10102"}]})
    }
    #[tokio::test]
    async fn strict_configuration_validates_before_network_io() {
        let good = config();
        assert!(serde_json::from_value::<Config>(good.clone())
            .unwrap()
            .client()
            .is_ok());
        for field in ["version", "timeout_ms", "metadata_node"] {
            let mut bad = good.clone();
            bad[field] = json!(0);
            assert!(serde_json::from_value::<Config>(bad)
                .unwrap()
                .client()
                .is_err());
        }
        let mut bad = good.clone();
        bad["timeout_ms"] = json!(60_001);
        assert!(serde_json::from_value::<Config>(bad)
            .unwrap()
            .client()
            .is_err());
        let mut bad = good.clone();
        bad["unexpected"] = json!(true);
        assert!(serde_json::from_value::<Config>(bad).is_err());
        let mut bad = good.clone();
        bad["metadata"]["unexpected"] = json!(true);
        assert!(serde_json::from_value::<Config>(bad).is_err());
        for field in ["node", "group"] {
            let mut bad = good.clone();
            bad["metadata_nodes"][0][field] = json!(2);
            assert!(serde_json::from_value::<Config>(bad)
                .unwrap()
                .client()
                .is_err());
        }
        let mut bad = good.clone();
        bad["data_nodes"][0]["endpoint"] = good["metadata_nodes"][0]["endpoint"].clone();
        assert!(serde_json::from_value::<Config>(bad)
            .unwrap()
            .client()
            .is_err());
        let mut bad = good.clone();
        bad["data_nodes"][0]["group"] = json!(1);
        assert!(serde_json::from_value::<Config>(bad)
            .unwrap()
            .client()
            .is_err());
        let mut bad = good.clone();
        bad["data_nodes"]
            .as_array_mut()
            .unwrap()
            .push(good["data_nodes"][0].clone());
        assert!(serde_json::from_value::<Config>(bad)
            .unwrap()
            .client()
            .is_err());
        for address in ["127.0.0.1:0", "0.0.0.0:1234"] {
            let mut bad = good.clone();
            bad["data_nodes"][0]["endpoint"] = json!(address);
            assert!(serde_json::from_value::<Config>(bad)
                .unwrap()
                .client()
                .is_err());
        }
    }
    #[tokio::test]
    async fn help_and_check_work_offline_and_require_explicit_opt_in() {
        assert_eq!(run(vec!["--help".into()]).await.unwrap()["ok"], true);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("client.json");
        std::fs::write(&file, serde_json::to_vec(&config()).unwrap()).unwrap();
        let mut args = vec![
            "--config".into(),
            file.to_str().unwrap().into(),
            "config-check".into(),
        ];
        assert!(run(args.clone()).await.is_err());
        args.push("--allow-insecure-development".into());
        assert_eq!(run(args.clone()).await.unwrap()["network_contacted"], false);
        args.push("--allow-insecure-development".into());
        assert!(run(args).await.is_err());
    }
    #[test]
    fn strict_command_and_payload_errors() {
        for text in [
            "1",
            "-1:2",
            "1:-2",
            "1:2:3",
            "4294967296:1",
            "1:18446744073709551616",
            "1:+2",
        ] {
            assert!(vertex_ref(text).is_err(), "{text}");
        }
        assert_eq!(
            vertex_ref("10:1").unwrap(),
            VertexRef {
                partition: 10,
                id: 1
            }
        );
        assert!(parse_command(
            &["config-check".into()],
            BTreeMap::from([("--typo".into(), "1".into())])
        )
        .is_err());
        assert!(parse_command(
            &["experimental".into(), "vertex".into(), "delete".into()],
            BTreeMap::new()
        )
        .is_err());
        assert!(serde_json::from_str::<Config>(r#"{"version":1,"version":1}"#).is_err());
        assert!(serde_json::from_str::<VertexFile>(
            r#"{"version":1,"id":1,"labels":[],"properties":{},"typo":1}"#
        )
        .is_err());
        assert!(serde_json::from_str::<VertexFile>(
            r#"{"version":1,"id":1,"labels":[],"properties":{"x":{"Integer":9223372036854775808}}}"#
        )
        .is_err());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.json");
        std::fs::write(&path, vec![b' '; MAX_INPUT as usize + 1]).unwrap();
        assert!(read_json::<Json>(path.to_str().unwrap()).is_err());
    }
    #[test]
    fn qualified_rows_and_typed_values_are_lossless() {
        let row = ObservedRowV3::Path(vec![
            ObservedPathElement::Vertex(VertexRef {
                partition: 10,
                id: 1,
            }),
            ObservedPathElement::Edge(velagraph::experimental_graph::EdgeRef {
                partition: 10,
                id: 7,
            }),
            ObservedPathElement::Vertex(VertexRef {
                partition: 20,
                id: 1,
            }),
        ]);
        assert_eq!(
            observed_row(row),
            json!({"type":"path","elements":[{"type":"vertex","partition":10,"id":1},{"type":"edge","partition":10,"id":7},{"type":"vertex","partition":20,"id":1}]})
        );
        let source = ObservedElementRef::Edge(velagraph::experimental_graph::EdgeRef {
            partition: 10,
            id: u64::MAX,
        });
        assert_eq!(
            observed_row(ObservedRowV3::Value {
                source,
                value: Value::Integer(i64::MAX)
            }),
            json!({"type":"value","source":{"type":"edge","partition":10,"id":u64::MAX},"value":{"Integer":i64::MAX}})
        );
        assert_eq!(
            observed_row(ObservedRowV3::PropertyMap {
                source: VertexRef {
                    partition: 20,
                    id: 1
                },
                properties: BTreeMap::from([
                    ("name".into(), vec![Value::Text("界".into())]),
                    ("nullable".into(), vec![Value::Null]),
                ]),
            }),
            json!({"type":"property_map","source":{"type":"vertex","partition":20,"id":1},"properties":{"name":[{"Text":"界"}],"nullable":["Null"]}})
        );
        assert_eq!(
            v1_row(20, GremlinRow::Vertex(1)),
            json!({"type":"vertex","partition":20,"id":1})
        );
        assert_eq!(
            stamps(&[ShardReadStamp {
                group: 2,
                shard: 102,
                epoch: 3,
                sequence: 4
            }]),
            vec![json!({"group":2,"shard":102,"epoch":3,"sequence":4})]
        );
    }
    #[test]
    fn failed_stage_retains_original_attempt_semantics_and_ids() {
        use replicated_graph::routing::RouteError;
        use velagraph::StorageNodeError;
        for (stage, outcome, ack) in [
            (Stage::Source, StageOutcome::Unknown, false),
            (Stage::Reverse, StageOutcome::Unknown, true),
            (Stage::Reverse, StageOutcome::Incomplete, true),
            (Stage::Source, StageOutcome::Rejected, false),
        ] {
            let error = ExperimentalRouteError::Stage {
                stage,
                outcome,
                request_id: [9; 16],
                source_acknowledged_this_attempt: ack,
                error: RouteError::Storage(StorageNodeError::Timeout {
                    request_id: Some([9; 16]),
                }),
            };
            let failure = append_failure(
                &error,
                &request_id([1; 16]),
                &Some(request_id([9; 16])),
                &element("edge", 10, 7),
            );
            let json = error_json(&failure.into());
            assert_eq!(json["ok"], false);
            assert_eq!(
                json["error"]["request_id"],
                "09090909090909090909090909090909"
            );
            assert_eq!(
                json["error"]["source_request_id"],
                "01010101010101010101010101010101"
            );
            assert_eq!(json["error"]["stage"], format!("{stage:?}"));
            assert_eq!(json["error"]["outcome"], format!("{outcome:?}"));
            assert_eq!(json["error"]["source_acknowledged_this_attempt"], ack);
            assert_eq!(json["error"]["retry_supported"], false);
        }
    }
}

#[cfg(test)]
mod additional_tests {
    use super::*;
    #[test]
    fn every_observed_shape_keeps_unverified_and_snapshot_vector() {
        for rows in [
            vec![],
            vec![ObservedRowV3::Count(0)],
            vec![ObservedRowV3::Value {
                source: ObservedElementRef::Vertex(VertexRef {
                    partition: 20,
                    id: 1,
                }),
                value: Value::Null,
            }],
        ] {
            let response = observed_json(ObservedTraversalV3 {
                rows,
                completeness: AdjacencyCompleteness::Unverified,
                metadata_sequence: 12,
                snapshots: vec![ShardReadStamp {
                    group: 2,
                    shard: 102,
                    epoch: 1,
                    sequence: 4,
                }],
            });
            assert_eq!(response["completeness"], "Unverified");
            assert_eq!(response["metadata_sequence"], 12);
            assert_eq!(
                response["snapshots"],
                json!([{"group":2,"shard":102,"epoch":1,"sequence":4}])
            );
        }
    }
    #[test]
    fn duplicate_properties_fail_instead_of_overwriting() {
        assert!(serde_json::from_str::<VertexFile>(
            r#"{"version":1,"id":1,"labels":[],"properties":{"x":{"Integer":1},"x":{"Integer":2}}}"#
        )
        .is_err());
    }
    #[tokio::test]
    async fn server_query_is_explicit_and_unknown_replica_fails_offline() {
        for namespace in ["v1", "experimental"] {
            let options = BTreeMap::from([
                ("--group".into(), "2".into()),
                ("--node".into(), "1".into()),
                ("--shard".into(), "7".into()),
                ("--partition".into(), "10".into()),
                ("--query".into(), "g.V().count()".into()),
            ]);
            let words = vec![namespace.into(), "server-query".into()];
            let parsed = parse_command(&words, options.clone()).unwrap();
            assert!(matches!(
                parsed,
                Command::ServerQuery {
                    group: 2,
                    node: 1,
                    shard: 7,
                    partition: 10,
                    ..
                }
            ));
            for field in ["--group", "--node", "--shard", "--partition", "--query"] {
                let mut missing = options.clone();
                missing.remove(field);
                assert!(parse_command(&words, missing).is_err());
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("client.json");
        std::fs::write(&file, serde_json::to_vec(&super::tests::config()).unwrap()).unwrap();
        let args = [
            "--config",
            file.to_str().unwrap(),
            "--allow-insecure-development",
            "v1",
            "server-query",
            "--group",
            "999",
            "--node",
            "1",
            "--shard",
            "7",
            "--partition",
            "10",
            "--query",
            "g.V().count()",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        let error = run(args).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "explicit query replica is not in data_nodes"
        );
    }
}
