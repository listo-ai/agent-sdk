//! Structured errors returned from a [`NodeBehavior`](crate::NodeBehavior)
//! entry point.
//!
//! Panics must never cross the SDK boundary — the adapter catches them
//! and converts to [`NodeError::Panic`] so a bad kind cannot take down
//! the host.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum NodeError {
    /// Author-level logic error — bad config, bad input, bad state.
    #[error("node runtime error: {0}")]
    Runtime(String),

    /// A slot referenced by this node does not exist on its kind.
    #[error("unknown slot `{0}`")]
    UnknownSlot(String),

    /// Config could not be deserialised against the declared schema.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// The [`NodeBehavior`](crate::NodeBehavior) entry point panicked.
    /// Produced by the SDK adapter, not by author code.
    #[error("node panicked: {0}")]
    Panic(String),
}

impl NodeError {
    pub fn runtime(msg: impl Into<String>) -> Self {
        Self::Runtime(msg.into())
    }
}
