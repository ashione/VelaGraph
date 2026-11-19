//! Typed parameter binding over an officially parsed immutable query shape.
use super::{
    GremlinQuery, GremlinStart, GremlinStep, QueryTextSize, MAX_GREMLIN_QUERY_BYTES,
    MAX_GREMLIN_STRUCTURAL_MARKS,
};
use crate::{Error, Result, Value};
use std::collections::HashSet;
use std::mem::{discriminant, Discriminant};

#[derive(Debug, Clone, Default)]
/// Parameter slots declared over a query parsed by the official grammar.
pub struct GremlinParameters {
    /// Replace the entire explicit V/E start list on each bind; preserve kind.
    pub start_ids: bool,
    /// Indices of Has steps whose existing literal values become parameters.
    /// The declaration order determines the order of binding values.
    pub has_value_steps: Vec<usize>,
}
#[derive(Debug, Clone, Copy, Default)]
/// Typed values for exactly the slots declared by a prepared query.
pub struct GremlinBindings<'a> {
    /// Required and nonempty when the prepared shape declares start-ID binding.
    pub start_ids: Option<&'a [u64]>,
    /// Has literals, in declaration order and with the template's value types.
    pub values: &'a [Value],
}
#[derive(Debug, Clone)]
/// Immutable query shape, without a data view, budget or execution state.
pub struct PreparedGremlinQuery {
    template: GremlinQuery,
    bind_start_ids: bool,
    value_slots: Vec<(usize, Discriminant<Value>)>,
    // Canonical syntax, keys and static literals, with dynamic contributions
    // removed. The bound query need not be rendered again for admission.
    fixed_size: QueryTextSize,
}
fn invalid(message: &str) -> Error {
    Error::UnsupportedGremlinArgument(format!("query bindings: {message}"))
}
impl GremlinQuery {
    /// Declare checked parameter slots without introducing placeholder syntax.
    pub fn prepare(self, parameters: GremlinParameters) -> Result<PreparedGremlinQuery> {
        if parameters.has_value_steps.len() > self.steps.len() {
            return Err(invalid("too many value slots"));
        }
        let mut seen = HashSet::new();
        let mut slots = Vec::new();
        for index in parameters.has_value_steps {
            if !seen.insert(index) {
                return Err(invalid("duplicate value slot"));
            }
            match self.steps.get(index) {
                Some(GremlinStep::Has {
                    value: Some(value), ..
                }) => slots.push((index, discriminant(value))),
                _ => {
                    return Err(invalid(
                        "value slot must refer to a Has step with a literal",
                    ))
                }
            }
        }
        let mut fixed_size = QueryTextSize::measure(&canonical_text(&self)?);
        if parameters.start_ids {
            fixed_size = fixed_size.subtract(start_size(&self.start));
        }
        for (index, _) in &slots {
            let GremlinStep::Has {
                value: Some(value), ..
            } = &self.steps[*index]
            else {
                unreachable!("slot validated above")
            };
            fixed_size = fixed_size.subtract(QueryTextSize::measure(&literal(value)?));
        }
        Ok(PreparedGremlinQuery {
            fixed_size,
            template: self,
            bind_start_ids: parameters.start_ids,
            value_slots: slots,
        })
    }
}
impl PreparedGremlinQuery {
    /// Clone and bind a shape, enforcing types and the textual admission limits.
    /// Start IDs retain input order and multiplicity. Empty bindings reject.
    pub fn bind(&self, bindings: GremlinBindings<'_>) -> Result<GremlinQuery> {
        if bindings.start_ids.is_some() != self.bind_start_ids {
            return Err(invalid("start binding does not match declaration"));
        }
        if bindings.values.len() != self.value_slots.len() {
            return Err(invalid("value binding count does not match declaration"));
        }
        if let Some(ids) = bindings.start_ids {
            if ids.is_empty() {
                return Err(invalid("empty start binding is not a global scan"));
            }
            if ids.len() > MAX_GREMLIN_STRUCTURAL_MARKS {
                return Err(invalid("too many bound start IDs"));
            }
            if ids.iter().any(|id| i64::try_from(*id).is_err()) {
                return Err(invalid("start IDs exceed textual integer range"));
            }
        }
        // Bound caller input before cloning or rendering it. The final canonical
        // query then passes the same text/structural admission as textual input.
        let mut input_bytes = 0usize;
        for (value, (_, kind)) in bindings.values.iter().zip(&self.value_slots) {
            if discriminant(value) != *kind {
                return Err(invalid("literal type mismatch"));
            }
            if matches!(value,Value::Real(x) if !x.is_finite()) {
                return Err(invalid("non-finite real"));
            }
            let bytes = match value {
                Value::Text(s) => s.len(),
                _ => std::mem::size_of::<Value>(),
            };
            input_bytes = input_bytes
                .checked_add(bytes)
                .ok_or_else(|| invalid("binding byte overflow"))?;
            if input_bytes > MAX_GREMLIN_QUERY_BYTES {
                return Err(invalid("binding values exceed query byte limit"));
            }
        }
        let mut size = self.fixed_size;
        if let Some(ids) = bindings.start_ids {
            size = size.add(ids_size(ids));
        }
        for value in bindings.values {
            size = size.add(QueryTextSize::measure(&literal(value)?));
        }
        // Canonical syntax comes from a parsed template. Dynamic IDs are decimal
        // integers and literals use the same JSON quoting as canonical_text, so
        // bindings cannot introduce an unquoted semicolon or a new traversal.
        size.validate()?;
        let mut query = self.template.clone();
        if let Some(ids) = bindings.start_ids {
            query.start = match query.start {
                GremlinStart::Vertices(_) => GremlinStart::Vertices(ids.to_vec()),
                GremlinStart::Edges(_) => GremlinStart::Edges(ids.to_vec()),
            };
        }
        for (value, (index, _)) in bindings.values.iter().zip(&self.value_slots) {
            let GremlinStep::Has { value: target, .. } = &mut query.steps[*index] else {
                unreachable!("slot validated at prepare")
            };
            *target = Some(value.clone());
        }
        Ok(query)
    }
}
fn ids_size(ids: &[u64]) -> QueryTextSize {
    let separators = ids.len().saturating_sub(1);
    QueryTextSize {
        bytes: separators
            + ids
                .iter()
                .map(|id| id.checked_ilog10().unwrap_or(0) as usize + 1)
                .sum::<usize>(),
        marks: separators,
    }
}
fn start_size(start: &GremlinStart) -> QueryTextSize {
    match start {
        GremlinStart::Vertices(ids) | GremlinStart::Edges(ids) => ids_size(ids),
    }
}

fn quote(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization")
}
fn literal(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Null => "null".into(),
        Value::Boolean(v) => v.to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Text(v) => quote(v),
        Value::Real(v) => {
            if !v.is_finite() {
                return Err(invalid("non-finite real"));
            }
            serde_json::to_string(v).expect("finite real serialization")
        }
    })
}
fn canonical_text(query: &GremlinQuery) -> Result<String> {
    #[cfg(feature = "query-profiling")]
    crate::query_profile::read_event(crate::query_profile::ReadEvent::PreparedTextRender);
    let (kind, ids) = match &query.start {
        GremlinStart::Vertices(ids) => ("V", ids),
        GremlinStart::Edges(ids) => ("E", ids),
    };
    let mut text = format!(
        "g.{kind}({})",
        ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
    );
    for step in &query.steps {
        let rendered = match step {
            GremlinStep::HasLabel(labels) => format!(
                "hasLabel({})",
                labels
                    .iter()
                    .map(|s| quote(s))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            GremlinStep::Has { key, value } => match value {
                Some(v) => format!("has({},{})", quote(key), literal(v)?),
                None => format!("has({})", quote(key)),
            },
            GremlinStep::Out(label) => {
                format!("out({})", label.as_deref().map(quote).unwrap_or_default())
            }
            GremlinStep::In(label) => {
                format!("in({})", label.as_deref().map(quote).unwrap_or_default())
            }
            GremlinStep::Both(label) => {
                format!("both({})", label.as_deref().map(quote).unwrap_or_default())
            }
            GremlinStep::Values(key) => format!("values({})", quote(key)),
            GremlinStep::Id => "id()".into(),
            GremlinStep::Label => "label()".into(),
            GremlinStep::Limit(n) => format!("limit({n})"),
            GremlinStep::Path => "path()".into(),
            GremlinStep::Count => "count()".into(),
        };
        text.push('.');
        text.push_str(&rendered);
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reference is the pre-optimization admission path: render the complete
    // bound query and run the ordinary source-text guard on those exact bytes.
    fn compare(prepared: &PreparedGremlinQuery, bindings: GremlinBindings<'_>) {
        let mut expected = prepared.template.clone();
        if let Some(ids) = bindings.start_ids {
            expected.start = match expected.start {
                GremlinStart::Vertices(_) => GremlinStart::Vertices(ids.to_vec()),
                GremlinStart::Edges(_) => GremlinStart::Edges(ids.to_vec()),
            };
        }
        for (value, (index, _)) in bindings.values.iter().zip(&prepared.value_slots) {
            let GremlinStep::Has { value: target, .. } = &mut expected.steps[*index] else {
                unreachable!()
            };
            *target = Some(value.clone());
        }
        let text = canonical_text(&expected).unwrap();
        let want = super::super::validate_query_text(&text);
        let got = prepared.bind(bindings);
        match (want, got) {
            (Ok(()), Ok(bound)) => assert_eq!(canonical_text(&bound).unwrap(), text),
            (Err(a), Err(b)) => assert_eq!(format!("{a:?}"), format!("{b:?}")),
            (a, b) => panic!("rendered admission differs: {a:?} vs {b:?}"),
        }
    }

    #[test]
    fn cached_admission_matches_full_render_for_all_literal_types_and_slots() {
        let templates = [
            (
                "g.V(1).has('x','a').both('LINK').limit(5).path()",
                Value::Text("a".into()),
            ),
            ("g.E(1).has('x',true).label()", Value::Boolean(true)),
            ("g.V(1).has('x',1).count()", Value::Integer(1)),
            ("g.V(1).has('x',1.5).id()", Value::Real(1.5)),
            ("g.V(1).has('x',null).values('y')", Value::Null),
        ];
        for (text, value) in templates {
            for bind_ids in [false, true] {
                let prepared = GremlinQuery::parse(text)
                    .unwrap()
                    .prepare(GremlinParameters {
                        start_ids: bind_ids,
                        has_value_steps: vec![0],
                    })
                    .unwrap();
                for count in [1, 2, 30, 63, 64] {
                    let ids = vec![i64::MAX as u64; count];
                    compare(
                        &prepared,
                        GremlinBindings {
                            start_ids: bind_ids.then_some(ids.as_slice()),
                            values: std::slice::from_ref(&value),
                        },
                    );
                }
            }
        }
        let real = GremlinQuery::parse("g.V(1).has('x',1.5).id()")
            .unwrap()
            .prepare(GremlinParameters {
                start_ids: true,
                has_value_steps: vec![0],
            })
            .unwrap();
        for value in [
            -0.0,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            f64::MAX,
            -f64::MAX,
            1e-300,
            1e300,
        ] {
            compare(
                &real,
                GremlinBindings {
                    start_ids: Some(&[0]),
                    values: &[Value::Real(value)],
                },
            );
        }
        let prepared = GremlinQuery::parse("g.V(0).has('a','old').has('b',1).id()")
            .unwrap()
            .prepare(GremlinParameters {
                start_ids: true,
                has_value_steps: vec![1, 0],
            })
            .unwrap();
        for value in [
            "plain",
            "汉字🙂",
            "\"\\\n\t\u{0000}",
            "');g.V(2);('",
            "[]{}(),.:;",
            "\\\";\\",
        ] {
            compare(
                &prepared,
                GremlinBindings {
                    start_ids: Some(&[0, 9, 10, 99, 100]),
                    values: &[Value::Integer(i64::MIN), Value::Text(value.into())],
                },
            );
        }
        for text in ["g.V().out().count()", "g.E(1).hasLabel('A','B').label()"] {
            let prepared = GremlinQuery::parse(text)
                .unwrap()
                .prepare(GremlinParameters::default())
                .unwrap();
            compare(&prepared, GremlinBindings::default());
        }
    }

    #[test]
    fn cached_admission_matches_exact_canonical_byte_and_mark_boundaries() {
        let prepared = GremlinQuery::parse("g.V(1).has('x','a').id()")
            .unwrap()
            .prepare(GremlinParameters {
                start_ids: true,
                has_value_steps: vec![0],
            })
            .unwrap();
        let empty = GremlinBindings {
            start_ids: Some(&[1]),
            values: &[Value::Text(String::new())],
        };
        let overhead = canonical_text(&prepared.bind(empty).unwrap())
            .unwrap()
            .len();
        for extra in [0, 1] {
            let value = Value::Text("x".repeat(MAX_GREMLIN_QUERY_BYTES - overhead + extra));
            let bindings = GremlinBindings {
                start_ids: Some(&[1]),
                values: std::slice::from_ref(&value),
            };
            assert_eq!(prepared.bind(bindings).is_ok(), extra == 0);
            compare(&prepared, bindings);
        }
        let marks =
            QueryTextSize::measure(&canonical_text(&prepared.bind(empty).unwrap()).unwrap()).marks;
        for extra in [0, 1] {
            let value = Value::Text(";".repeat(MAX_GREMLIN_STRUCTURAL_MARKS - marks + extra));
            let bindings = GremlinBindings {
                start_ids: Some(&[1]),
                values: std::slice::from_ref(&value),
            };
            assert_eq!(prepared.bind(bindings).is_ok(), extra == 0);
            compare(&prepared, bindings);
        }
        // JSON escaping expands canonical bytes without growing raw input beyond
        // its independent admission cap.
        for n in [2700, 2730, 2800, 8190] {
            compare(
                &prepared,
                GremlinBindings {
                    start_ids: Some(&[1]),
                    values: &[Value::Text("\u{0000}".repeat(n))],
                },
            );
        }
    }
}
