//! Lossless typed conversion; malformed peers fail closed before execution.
use super::*;
fn required<T>(v: Option<T>) -> Result<T, RemoteError> {
    v.ok_or(RemoteError::Protocol)
}
fn size(v: u64) -> Result<usize, RemoteError> {
    v.try_into().map_err(|_| RemoteError::Protocol)
}
impl From<RemoteIdentity> for wire::Identity {
    fn from(v: RemoteIdentity) -> Self {
        Self {
            group: v.group,
            node: v.node,
        }
    }
}
impl TryFrom<wire::Identity> for RemoteIdentity {
    type Error = RemoteError;
    fn try_from(v: wire::Identity) -> Result<Self, Self::Error> {
        Ok(Self {
            group: v.group,
            node: v.node,
        })
    }
}
impl From<QueryStamp> for wire::QueryStamp {
    fn from(v: QueryStamp) -> Self {
        Self {
            identity: Some(v.identity.into()),
            shard: v.shard,
            epoch: v.epoch,
            sequence: v.sequence,
        }
    }
}
impl TryFrom<wire::QueryStamp> for QueryStamp {
    type Error = RemoteError;
    fn try_from(v: wire::QueryStamp) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: required(v.identity)?.try_into()?,
            shard: v.shard,
            epoch: v.epoch,
            sequence: v.sequence,
        })
    }
}
impl From<Descriptor> for wire::Descriptor {
    fn from(v: Descriptor) -> Self {
        Self {
            stamp: Some(v.stamp.into()),
            incarnation: v.incarnation.to_vec(),
            token: v.token.to_vec(),
        }
    }
}
impl TryFrom<wire::Descriptor> for Descriptor {
    type Error = RemoteError;
    fn try_from(v: wire::Descriptor) -> Result<Self, Self::Error> {
        Ok(Self {
            stamp: required(v.stamp)?.try_into()?,
            incarnation: v
                .incarnation
                .try_into()
                .map_err(|_| RemoteError::Protocol)?,
            token: v.token.try_into().map_err(|_| RemoteError::Protocol)?,
        })
    }
}
impl From<Cost> for wire::Cost {
    fn from(v: Cost) -> Self {
        Self {
            bytes: v.bytes as u64,
            work: v.work as u64,
        }
    }
}
impl TryFrom<wire::Cost> for Cost {
    type Error = RemoteError;
    fn try_from(v: wire::Cost) -> Result<Self, Self::Error> {
        Ok(Self {
            bytes: size(v.bytes)?,
            work: size(v.work)?,
        })
    }
}
impl From<VertexId> for wire::VertexId {
    fn from(v: VertexId) -> Self {
        Self {
            partition: v.partition,
            id: v.id,
        }
    }
}
impl TryFrom<wire::VertexId> for VertexId {
    type Error = RemoteError;
    fn try_from(v: wire::VertexId) -> Result<Self, Self::Error> {
        Ok(Self {
            partition: v.partition,
            id: v.id,
        })
    }
}
impl From<EdgeHop> for wire::EdgeHop {
    fn from(v: EdgeHop) -> Self {
        Self {
            id: v.id,
            from: Some(v.from.into()),
            to: Some(v.to.into()),
            label: v.label,
        }
    }
}
impl TryFrom<wire::EdgeHop> for EdgeHop {
    type Error = RemoteError;
    fn try_from(v: wire::EdgeHop) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v.id,
            from: required(v.from)?.try_into()?,
            to: required(v.to)?.try_into()?,
            label: v.label,
        })
    }
}
impl From<IncomingEdge> for wire::IncomingEdge {
    fn from(v: IncomingEdge) -> Self {
        Self {
            id: v.id,
            partition: v.partition,
            label: v.label,
        }
    }
}
impl TryFrom<wire::IncomingEdge> for IncomingEdge {
    type Error = RemoteError;
    fn try_from(v: wire::IncomingEdge) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v.id,
            partition: v.partition,
            label: v.label,
        })
    }
}
impl From<Value> for wire::Value {
    fn from(v: Value) -> Self {
        use wire::value::Kind;
        Self {
            kind: Some(match v {
                Value::Null => Kind::Null(wire::Empty {}),
                Value::Integer(v) => Kind::Integer(v),
                Value::Real(v) => Kind::Real(v),
                Value::Text(v) => Kind::Text(v),
                Value::Boolean(v) => Kind::Boolean(v),
            }),
        }
    }
}
impl TryFrom<wire::Value> for Value {
    type Error = RemoteError;
    fn try_from(v: wire::Value) -> Result<Self, Self::Error> {
        use wire::value::Kind;
        Ok(match required(v.kind)? {
            Kind::Null(_) => Self::Null,
            Kind::Integer(v) => Self::Integer(v),
            Kind::Real(v) if v.is_finite() => Self::Real(v),
            Kind::Real(_) => return Err(RemoteError::Protocol),
            Kind::Text(v) => Self::Text(v),
            Kind::Boolean(v) => Self::Boolean(v),
        })
    }
}
impl From<scan::ScanStep> for wire::ScanStep {
    fn from(v: scan::ScanStep) -> Self {
        use wire::scan_step::Kind;
        Self {
            kind: Some(match v {
                scan::ScanStep::Has { key, value } => Kind::Has(wire::Filter {
                    key,
                    value: value.map(Into::into),
                }),
                scan::ScanStep::HasLabel(labels) => Kind::HasLabel(wire::Labels { labels }),
                scan::ScanStep::Limit(n) => Kind::Limit(n as u64),
            }),
        }
    }
}
impl TryFrom<wire::ScanStep> for scan::ScanStep {
    type Error = RemoteError;
    fn try_from(v: wire::ScanStep) -> Result<Self, Self::Error> {
        use wire::scan_step::Kind;
        Ok(match required(v.kind)? {
            Kind::Has(v) => Self::Has {
                key: v.key,
                value: v.value.map(TryInto::try_into).transpose()?,
            },
            Kind::HasLabel(v) => Self::HasLabel(v.labels),
            Kind::Limit(n) => Self::Limit(size(n)?),
        })
    }
}
impl From<scan::ScanRequest> for wire::ScanRequest {
    fn from(v: scan::ScanRequest) -> Self {
        Self {
            partition: v.partition,
            after: v.after,
            count: v.count,
            steps: v.steps.into_iter().map(Into::into).collect(),
        }
    }
}
impl TryFrom<wire::ScanRequest> for scan::ScanRequest {
    type Error = RemoteError;
    fn try_from(v: wire::ScanRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            partition: v.partition,
            after: v.after,
            count: v.count,
            steps: v
                .steps
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}
impl From<scan::ScanResult> for wire::ScanResult {
    fn from(v: scan::ScanResult) -> Self {
        Self {
            next_after: v.next_after,
            scanned: v.scanned as u64,
            matched: v.matched as u64,
            ids: v.ids,
            limits: v.limits.into_iter().map(|n| n as u64).collect(),
        }
    }
}
impl TryFrom<wire::ScanResult> for scan::ScanResult {
    type Error = RemoteError;
    fn try_from(v: wire::ScanResult) -> Result<Self, Self::Error> {
        Ok(Self {
            next_after: v.next_after,
            scanned: size(v.scanned)?,
            matched: size(v.matched)?,
            ids: v.ids,
            limits: v.limits.into_iter().map(size).collect::<Result<_, _>>()?,
        })
    }
}
impl From<Fragment> for wire::Fragment {
    fn from(v: Fragment) -> Self {
        use wire::fragment::Kind;
        Self {
            kind: Some(match v {
                Fragment::Exists(v) => Kind::Exists(v.into()),
                Fragment::Out(v) => Kind::Out(v.into()),
                Fragment::In(v) => Kind::In(v.into()),
                Fragment::VerifyReverse(v) => Kind::VerifyReverse(v.into()),
                Fragment::Scan(v) => Kind::Scan(v.into()),
                Fragment::HierarchyBatch {
                    partition,
                    fragments,
                } => Kind::HierarchyBatch(wire::HierarchyBatch {
                    partition,
                    fragments: fragments.into_iter().map(Into::into).collect(),
                }),
                Fragment::ResolveIncoming { target, edge } => {
                    Kind::ResolveIncoming(wire::ResolveIncoming {
                        target: Some(target.into()),
                        edge: Some(edge.into()),
                    })
                }
                Fragment::Has { vertex, key, value } => Kind::Has(wire::Has {
                    vertex: Some(vertex.into()),
                    key,
                    value: value.map(Into::into),
                }),
                Fragment::HasLabel { vertex, labels } => Kind::HasLabel(wire::HasLabel {
                    vertex: Some(vertex.into()),
                    labels,
                }),
                Fragment::Values { vertex, key } => Kind::Values(wire::Values {
                    vertex: Some(vertex.into()),
                    key,
                }),
                Fragment::Project { vertex, keys } => Kind::Project(wire::Project {
                    vertex: Some(vertex.into()),
                    keys,
                }),
            }),
        }
    }
}
impl TryFrom<wire::Fragment> for Fragment {
    type Error = RemoteError;
    fn try_from(v: wire::Fragment) -> Result<Self, Self::Error> {
        use wire::fragment::Kind;
        let result = match required(v.kind)? {
            Kind::Exists(v) => Self::Exists(v.try_into()?),
            Kind::Out(v) => Self::Out(v.try_into()?),
            Kind::In(v) => Self::In(v.try_into()?),
            Kind::VerifyReverse(v) => Self::VerifyReverse(v.try_into()?),
            Kind::Scan(v) => Self::Scan(v.try_into()?),
            Kind::HierarchyBatch(v) => {
                if v.fragments
                    .iter()
                    .any(|f| matches!(f.kind, Some(wire::fragment::Kind::HierarchyBatch(_))))
                {
                    return Err(RemoteError::Protocol);
                }
                Self::HierarchyBatch {
                    partition: v.partition,
                    fragments: v
                        .fragments
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<_, _>>()?,
                }
            }
            Kind::ResolveIncoming(v) => Self::ResolveIncoming {
                target: required(v.target)?.try_into()?,
                edge: required(v.edge)?.try_into()?,
            },
            Kind::Has(v) => Self::Has {
                vertex: required(v.vertex)?.try_into()?,
                key: v.key,
                value: v.value.map(TryInto::try_into).transpose()?,
            },
            Kind::HasLabel(v) => Self::HasLabel {
                vertex: required(v.vertex)?.try_into()?,
                labels: v.labels,
            },
            Kind::Values(v) => Self::Values {
                vertex: required(v.vertex)?.try_into()?,
                key: v.key,
            },
            Kind::Project(v) => Self::Project {
                vertex: required(v.vertex)?.try_into()?,
                keys: v.keys,
            },
        };
        result.validate()?;
        Ok(result)
    }
}
impl From<FragmentRequest> for wire::FragmentRequest {
    fn from(v: FragmentRequest) -> Self {
        Self {
            context: None,
            descriptor: Some(v.descriptor.into()),
            fragment: Some(v.fragment.into()),
            remaining: Some(v.remaining.into()),
        }
    }
}
impl TryFrom<wire::FragmentRequest> for FragmentRequest {
    type Error = RemoteError;
    fn try_from(v: wire::FragmentRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            descriptor: required(v.descriptor)?.try_into()?,
            fragment: required(v.fragment)?.try_into()?,
            remaining: required(v.remaining)?.try_into()?,
        })
    }
}
impl From<FragmentValue> for wire::FragmentValue {
    fn from(v: FragmentValue) -> Self {
        use wire::fragment_value::Kind;
        Self {
            kind: Some(match v {
                FragmentValue::Bool(v) => Kind::Boolean(v),
                FragmentValue::Scan(v) => Kind::Scan(v.into()),
                FragmentValue::Edge(v) => Kind::Edge(v.into()),
                FragmentValue::Value(v) => Kind::Value(wire::OptionalValue {
                    value: v.map(Into::into),
                }),
                FragmentValue::HierarchyBatch(v) => Kind::HierarchyBatch(wire::BatchValues {
                    values: v.into_iter().map(Into::into).collect(),
                }),
                FragmentValue::Edges(v) => Kind::Edges(wire::Edges {
                    edges: v.into_iter().map(Into::into).collect(),
                }),
                FragmentValue::Incoming(v) => Kind::Incoming(wire::Incoming {
                    edges: v.into_iter().map(Into::into).collect(),
                }),
                FragmentValue::Projected { vertex, properties } => {
                    Kind::Projected(wire::Projected {
                        vertex: Some(vertex.into()),
                        properties: properties
                            .into_iter()
                            .map(|(key, value)| wire::Property {
                                key,
                                value: Some(value.into()),
                            })
                            .collect(),
                    })
                }
            }),
        }
    }
}
impl TryFrom<wire::FragmentValue> for FragmentValue {
    type Error = RemoteError;
    fn try_from(v: wire::FragmentValue) -> Result<Self, Self::Error> {
        use wire::fragment_value::Kind;
        Ok(match required(v.kind)? {
            Kind::Boolean(v) => Self::Bool(v),
            Kind::Scan(v) => Self::Scan(v.try_into()?),
            Kind::Edge(v) => Self::Edge(v.try_into()?),
            Kind::Value(v) => Self::Value(v.value.map(TryInto::try_into).transpose()?),
            Kind::HierarchyBatch(v) => {
                if v.values
                    .iter()
                    .any(|f| matches!(f.kind, Some(wire::fragment_value::Kind::HierarchyBatch(_))))
                {
                    return Err(RemoteError::Protocol);
                }
                Self::HierarchyBatch(
                    v.values
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<_, _>>()?,
                )
            }
            Kind::Edges(v) => Self::Edges(
                v.edges
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            ),
            Kind::Incoming(v) => Self::Incoming(
                v.edges
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            ),
            Kind::Projected(v) => {
                let mut properties = BTreeMap::new();
                for p in v.properties {
                    if properties
                        .insert(p.key, required(p.value)?.try_into()?)
                        .is_some()
                    {
                        return Err(RemoteError::Protocol);
                    }
                }
                Self::Projected {
                    vertex: required(v.vertex)?.try_into()?,
                    properties,
                }
            }
        })
    }
}
impl From<FragmentOutput> for wire::FragmentOutput {
    fn from(v: FragmentOutput) -> Self {
        Self {
            descriptor: Some(v.descriptor.into()),
            cost: Some(v.cost.into()),
            value: Some(v.value.into()),
        }
    }
}
impl TryFrom<wire::FragmentOutput> for FragmentOutput {
    type Error = RemoteError;
    fn try_from(v: wire::FragmentOutput) -> Result<Self, Self::Error> {
        Ok(Self {
            descriptor: required(v.descriptor)?.try_into()?,
            cost: required(v.cost)?.try_into()?,
            value: required(v.value)?.try_into()?,
        })
    }
}
impl From<QueryFailure> for wire::QueryFailure {
    fn from(v: QueryFailure) -> Self {
        match v {
            QueryFailure::Parse => Self::Parse,
            QueryFailure::Unsupported => Self::Unsupported,
            QueryFailure::InvalidArgument => Self::InvalidArgument,
            QueryFailure::ResourceExhausted => Self::ResourceExhausted,
            QueryFailure::Timeout => Self::Timeout,
            QueryFailure::UnsupportedScope => Self::UnsupportedScope,
            QueryFailure::IncompleteOrCorrupt => Self::IncompleteOrCorrupt,
            QueryFailure::Unavailable => Self::Unavailable,
        }
    }
}
impl TryFrom<wire::QueryFailure> for QueryFailure {
    type Error = RemoteError;
    fn try_from(v: wire::QueryFailure) -> Result<Self, Self::Error> {
        Ok(match v {
            wire::QueryFailure::Parse => Self::Parse,
            wire::QueryFailure::Unsupported => Self::Unsupported,
            wire::QueryFailure::InvalidArgument => Self::InvalidArgument,
            wire::QueryFailure::ResourceExhausted => Self::ResourceExhausted,
            wire::QueryFailure::Timeout => Self::Timeout,
            wire::QueryFailure::UnsupportedScope => Self::UnsupportedScope,
            wire::QueryFailure::IncompleteOrCorrupt => Self::IncompleteOrCorrupt,
            wire::QueryFailure::Unavailable => Self::Unavailable,
            wire::QueryFailure::Unspecified => return Err(RemoteError::Protocol),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    fn vertex() -> VertexId {
        VertexId {
            partition: 3,
            id: u64::MAX,
        }
    }
    fn edge() -> EdgeHop {
        EdgeHop {
            id: u64::MAX,
            from: vertex(),
            to: vertex(),
            label: "reports_to".into(),
        }
    }
    fn incoming() -> IncomingEdge {
        IncomingEdge {
            id: u64::MAX,
            partition: 3,
            label: "reports_to".into(),
        }
    }
    fn descriptor() -> Descriptor {
        Descriptor {
            stamp: QueryStamp {
                identity: RemoteIdentity { group: 7, node: 9 },
                shard: 5,
                epoch: 8,
                sequence: u64::MAX,
            },
            incarnation: [1; 16],
            token: [2; 16],
        }
    }
    #[test]
    fn all_property_types_roundtrip_without_coercion() {
        for value in [
            Value::Null,
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            Value::Real(-0.0),
            Value::Real(1.125),
            Value::Text("中文\0".into()),
            Value::Boolean(false),
        ] {
            let wire: wire::Value = value.into();
            let bytes = wire.encode_to_vec();
            let decoded = Value::try_from(wire).unwrap();
            assert_eq!(wire::Value::from(decoded).encode_to_vec(), bytes);
        }
        for n in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(Value::try_from(wire::Value {
                kind: Some(wire::value::Kind::Real(n))
            })
            .is_err());
        }
        assert!(Value::try_from(wire::Value { kind: None }).is_err());
    }
    #[test]
    fn every_fragment_roundtrips_typed() {
        let fragments = vec![
            Fragment::HierarchyBatch {
                partition: 3,
                fragments: vec![Fragment::Exists(vertex())],
            },
            Fragment::Scan(scan::ScanRequest {
                partition: 3,
                after: Some(u64::MAX),
                steps: vec![
                    scan::ScanStep::Has {
                        key: "x".into(),
                        value: Some(Value::Null),
                    },
                    scan::ScanStep::HasLabel(vec!["person".into()]),
                    scan::ScanStep::Limit(100),
                ],
                count: false,
            }),
            Fragment::Exists(vertex()),
            Fragment::Out(vertex()),
            Fragment::In(vertex()),
            Fragment::ResolveIncoming {
                target: vertex(),
                edge: incoming(),
            },
            Fragment::VerifyReverse(edge()),
            Fragment::Has {
                vertex: vertex(),
                key: "x".into(),
                value: None,
            },
            Fragment::HasLabel {
                vertex: vertex(),
                labels: vec!["person".into()],
            },
            Fragment::Values {
                vertex: vertex(),
                key: "x".into(),
            },
            Fragment::Project {
                vertex: vertex(),
                keys: vec!["x".into()],
            },
        ];
        for fragment in fragments {
            let request = FragmentRequest {
                descriptor: descriptor(),
                fragment,
                remaining: Cost {
                    bytes: MAX_BYTES,
                    work: MAX_WORK,
                },
            };
            let wire: wire::FragmentRequest = request.into();
            assert!(wire.context.is_none());
            let bytes = wire.encode_to_vec();
            let decoded = FragmentRequest::try_from(wire).unwrap();
            assert_eq!(decoded.descriptor, descriptor());
            assert_eq!(wire::FragmentRequest::from(decoded).encode_to_vec(), bytes);
        }
    }
    #[test]
    fn every_output_and_failure_roundtrips_typed() {
        let values = vec![
            FragmentValue::HierarchyBatch(vec![FragmentValue::Bool(true)]),
            FragmentValue::Scan(scan::ScanResult {
                next_after: Some(u64::MAX),
                scanned: 128,
                matched: 1,
                ids: vec![u64::MAX],
                limits: vec![100],
            }),
            FragmentValue::Bool(false),
            FragmentValue::Edges(vec![edge()]),
            FragmentValue::Incoming(vec![incoming()]),
            FragmentValue::Edge(edge()),
            FragmentValue::Value(None),
            FragmentValue::Value(Some(Value::Null)),
            FragmentValue::Projected {
                vertex: vertex(),
                properties: BTreeMap::from([("x".into(), Value::Integer(i64::MIN))]),
            },
        ];
        for value in values {
            let wire: wire::FragmentOutput = FragmentOutput {
                descriptor: descriptor(),
                cost: Cost {
                    bytes: MAX_BYTES,
                    work: MAX_WORK,
                },
                value,
            }
            .into();
            let bytes = wire.encode_to_vec();
            assert_eq!(
                wire::FragmentOutput::from(FragmentOutput::try_from(wire).unwrap()).encode_to_vec(),
                bytes
            );
        }
        for failure in [
            QueryFailure::Parse,
            QueryFailure::Unsupported,
            QueryFailure::InvalidArgument,
            QueryFailure::ResourceExhausted,
            QueryFailure::Timeout,
            QueryFailure::UnsupportedScope,
            QueryFailure::IncompleteOrCorrupt,
            QueryFailure::Unavailable,
        ] {
            assert_eq!(
                QueryFailure::try_from(wire::QueryFailure::from(failure)).unwrap(),
                failure
            );
        }
        assert!(QueryFailure::try_from(wire::QueryFailure::Unspecified).is_err());
        assert!(wire::QueryFailure::try_from(99).is_err());
    }
    #[test]
    fn malformed_structures_fail_closed() {
        assert!(Descriptor::try_from(wire::Descriptor::default()).is_err());
        let mut d: wire::Descriptor = descriptor().into();
        d.token.pop();
        assert!(Descriptor::try_from(d).is_err());
        assert!(FragmentRequest::try_from(wire::FragmentRequest::default()).is_err());
        assert!(Fragment::try_from(wire::Fragment::default()).is_err());
        assert!(FragmentOutput::try_from(wire::FragmentOutput::default()).is_err());
        let nested = Fragment::HierarchyBatch {
            partition: 3,
            fragments: vec![Fragment::HierarchyBatch {
                partition: 3,
                fragments: vec![Fragment::Exists(vertex())],
            }],
        };
        assert!(Fragment::try_from(wire::Fragment::from(nested)).is_err());
        let nested = FragmentValue::HierarchyBatch(vec![FragmentValue::HierarchyBatch(vec![
            FragmentValue::Bool(true),
        ])]);
        assert!(FragmentValue::try_from(wire::FragmentValue::from(nested)).is_err());
        let prop = wire::Property {
            key: "x".into(),
            value: Some(Value::Null.into()),
        };
        let value = wire::FragmentValue {
            kind: Some(wire::fragment_value::Kind::Projected(wire::Projected {
                vertex: Some(vertex().into()),
                properties: vec![prop.clone(), prop],
            })),
        };
        assert!(FragmentValue::try_from(value).is_err());
        let invalid = Fragment::Scan(scan::ScanRequest {
            partition: 3,
            after: None,
            steps: vec![scan::ScanStep::Limit(1001)],
            count: false,
        });
        assert!(Fragment::try_from(wire::Fragment::from(invalid)).is_err());
    }
}
