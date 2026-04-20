//! `ResolvedSettings<T>` — per-message merge of (schema defaults,
//! persisted config, message overrides).
//!
//! Resolution order, lowest to highest priority:
//!   1. `settings_schema` defaults from the manifest
//!   2. persisted node config (the JSON blob held by `NodeCtx::config`)
//!   3. fields named in `manifest.msg_overrides`, taken from `msg.metadata`
//!
//! The merged value is then deserialised into the author's `T` so that
//! a behaviour body works against a typed struct, not a JSON map.
//!
//! See `docs/sessions/NODE-SCOPE.md` § "Settings & runtime overrides".

use std::ops::Deref;

use serde::de::DeserializeOwned;
use serde_json::{Map, Value as JsonValue};
use spi::Msg;

use crate::ctx::NodeCtx;
use crate::error::NodeError;

/// Wrapper carrying the deserialised, merged settings object. Deref to
/// `T` so authors can use it like the bare struct.
#[derive(Debug, Clone)]
pub struct ResolvedSettings<T> {
    inner: T,
}

impl<T> ResolvedSettings<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> Deref for ResolvedSettings<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl NodeCtx {
    /// Resolve the typed settings struct for the current invocation.
    pub fn resolve_settings<T: DeserializeOwned>(
        &self,
        msg: &Msg,
    ) -> Result<ResolvedSettings<T>, NodeError> {
        let merged = merge(
            schema_defaults(&self.manifest().settings_schema),
            self.config().clone(),
            apply_msg_overrides(&self.manifest().msg_overrides, msg),
        );
        let inner: T =
            serde_json::from_value(merged).map_err(|e| NodeError::InvalidConfig(e.to_string()))?;
        Ok(ResolvedSettings::new(inner))
    }
}

fn schema_defaults(schema: &JsonValue) -> JsonValue {
    let Some(props) = schema.get("properties").and_then(JsonValue::as_object) else {
        return JsonValue::Object(Map::new());
    };
    let mut out = Map::new();
    for (k, v) in props {
        if let Some(default) = v.get("default") {
            out.insert(k.clone(), default.clone());
        }
    }
    JsonValue::Object(out)
}

fn apply_msg_overrides(
    overrides: &std::collections::BTreeMap<String, String>,
    msg: &Msg,
) -> JsonValue {
    let mut out = Map::new();
    for (settings_field, msg_key) in overrides {
        if let Some(v) = msg.metadata.get(msg_key) {
            out.insert(settings_field.clone(), v.clone());
        }
    }
    JsonValue::Object(out)
}

fn merge(defaults: JsonValue, config: JsonValue, overrides: JsonValue) -> JsonValue {
    let mut out = match defaults {
        JsonValue::Object(m) => m,
        _ => Map::new(),
    };
    if let JsonValue::Object(c) = config {
        for (k, v) in c {
            out.insert(k, v);
        }
    }
    if let JsonValue::Object(o) = overrides {
        for (k, v) in o {
            out.insert(k, v);
        }
    }
    JsonValue::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_extracted_from_json_schema() {
        let s = json!({
            "type": "object",
            "properties": {
                "step": { "type": "integer", "default": 1 },
                "wrap": { "type": "boolean", "default": false },
                "min":  { "type": ["integer", "null"] }
            }
        });
        let d = schema_defaults(&s);
        assert_eq!(d, json!({ "step": 1, "wrap": false }));
    }

    #[test]
    fn overrides_beat_config_beats_defaults() {
        let merged = merge(
            json!({ "step": 1, "initial": 0 }),
            json!({ "step": 5 }),
            json!({ "initial": 99 }),
        );
        assert_eq!(merged, json!({ "step": 5, "initial": 99 }));
    }
}
