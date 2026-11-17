//! Typed parameter binding over an officially parsed immutable query shape.
use super::{
    validate_query_text, GremlinQuery, GremlinStart, GremlinStep, MAX_GREMLIN_QUERY_BYTES,
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
        Ok(PreparedGremlinQuery {
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
        validate_query_text(&canonical_text(&query)?)?;
        Ok(query)
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
