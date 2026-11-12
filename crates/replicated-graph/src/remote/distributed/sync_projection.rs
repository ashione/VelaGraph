//! Terminal selected-property projection for the bounded synchronous coordinator.
//! No new allowance, snapshot, retry or hierarchy-job admission is introduced.
use super::*;

pub(super) use crate::selected_properties::valid_map;

impl Execution<'_> {
    pub(super) async fn project_rows(
        &mut self,
        rows: &[Traverser],
        keys: &[String],
    ) -> Result<Vec<QueryRow>, QueryFailure> {
        projection::validate_keys(keys)?;
        // The map stream is additional to the retained traversal stream.
        // Charge its complete backing allocation before reserving it, including
        // when the remaining allowance would fail on the first result.
        self.cost.charge(
            rows.len() * std::mem::size_of::<QueryRow>(),
            0,
            self.ceiling,
        )?;
        let mut output = Vec::with_capacity(rows.len());
        for row in rows {
            self.check()?;
            let FragmentValue::Projected { vertex, properties } = self
                .fragment(Fragment::Project {
                    vertex: row.vertex,
                    keys: keys.to_vec(),
                })
                .await?
            else {
                return Err(QueryFailure::IncompleteOrCorrupt);
            };
            if vertex != row.vertex || properties.keys().any(|key| !keys.contains(key)) {
                return Err(QueryFailure::IncompleteOrCorrupt);
            }
            let bytes = projection::properties_bytes(&properties)?;
            self.cost
                .charge(bytes + 128 * properties.len() + 256, 1, self.ceiling)?;
            output.push(QueryRow::PropertyMap {
                source: vertex.element(),
                properties: properties
                    .into_iter()
                    .map(|(key, value)| (key, vec![value]))
                    .collect(),
            });
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selected_map_admission_and_untrusted_rows_fail_closed() {
        let request = |text: &str| DistributedRequest {
            start_partition: 1,
            text: text.into(),
            timeout_millis: 5000,
        };
        assert!(plan(&request(
            "g.V(1).out('member_of').valueMap('name','active')"
        ))
        .is_ok());
        for text in [
            "g.V(1).valueMap()",
            "g.V(1).valueMap('a','a')",
            "g.V(1).valueMap('a').count()",
            "g.V(1).valueMap('a').limit(1)",
            "g.V(1).values('a').valueMap('a')",
        ] {
            assert!(plan(&request(text)).is_err(), "{text}");
        }
        let keys = vec!["a".into()];
        assert!(valid_map(&BTreeMap::new(), &keys));
        assert!(valid_map(
            &BTreeMap::from([("a".into(), vec![Value::Null])]),
            &keys
        ));
        for values in [
            vec![],
            vec![Value::Null, Value::Null],
            vec![Value::Real(f64::NAN)],
            vec![Value::Text("x".repeat(2049))],
        ] {
            assert!(!valid_map(&BTreeMap::from([("a".into(), values)]), &keys));
        }
        assert!(!valid_map(
            &BTreeMap::from([("b".into(), vec![Value::Null])]),
            &keys
        ));
        let mut output = DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: vec![QueryStamp {
                identity: RemoteIdentity { group: 1, node: 1 },
                shard: 1,
                epoch: 1,
                sequence: 1,
            }],
            rows: vec![QueryRow::PropertyMap {
                source: QualifiedElement::Vertex {
                    partition: 1,
                    id: 1,
                },
                properties: BTreeMap::new(),
            }],
        };
        let req = request("g.V(1).valueMap('a')");
        assert!(validate_output(&output, &req).is_ok());
        assert!(validate_output(&output, &request("g.V(1)")).is_err());
        output.rows = vec![QueryRow::Element(QualifiedElement::Vertex {
            partition: 1,
            id: 1,
        })];
        assert!(validate_output(&output, &req).is_err());
        output.rows = vec![QueryRow::PropertyMap {
            source: QualifiedElement::Vertex {
                partition: 2,
                id: 1,
            },
            properties: BTreeMap::new(),
        }];
        assert!(validate_output(&output, &req).is_err());
    }
    #[tokio::test]
    async fn output_backing_allocation_is_charged_before_any_rpc() {
        let catalog = StaticCatalog {
            partitions: Vec::new(),
        };
        let mut execution = Execution {
            fixed_sessions: None,
            control: None,
            fixed_parallelism: 3,
            scan_cost: Cost::default(),
            catalog: &catalog,
            sessions: BTreeMap::new(),
            end: Instant::now() + Duration::from_secs(1),
            cost: Cost::default(),
            ceiling: Cost {
                bytes: std::mem::size_of::<QueryRow>() - 1,
                work: 100,
            },
        };
        let row = Traverser {
            vertex: VertexId {
                partition: 1,
                id: 1,
            },
            path: Vec::new(),
            value: None,
        };
        assert!(matches!(
            execution.project_rows(&[row], &["name".into()]).await,
            Err(QueryFailure::ResourceExhausted)
        ));
        assert_eq!(execution.cost.bytes, std::mem::size_of::<QueryRow>());
        assert_eq!(execution.cost.work, 0);
    }
}
