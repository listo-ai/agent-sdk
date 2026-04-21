//! Wasm-block guest SDK (feature `wasm`).
//!
//! The host speaks a three-function ABI (see
//! `blocks_host::wasm` for the contract). This module wraps it
//! so block authors write a [`WasmPlugin`] impl + one
//! [`export_plugin!`] invocation — never raw `extern "C"` /
//! pointer-packing.
//!
//! Minimal block:
//!
//! ```ignore
//! use blocks_sdk::wasm::{self, DescribeResponse, KindDecl, OnInputEnvelope, OutputMsg};
//!
//! struct MyPlugin;
//! impl wasm::WasmPlugin for MyPlugin {
//!     fn describe() -> DescribeResponse {
//!         DescribeResponse {
//!             extension_id: "com.acme.wasm-demo".into(),
//!             version: "0.1.0".into(),
//!             kinds: vec![KindDecl { kind_id: "com.acme.wasm-demo.double".into(), ..Default::default() }],
//!         }
//!     }
//!     fn on_input(env: OnInputEnvelope<'_>) -> Result<Vec<OutputMsg>, String> {
//!         let n: f64 = serde_json::from_str(env.msg_json).map_err(|e| e.to_string())?;
//!         Ok(vec![OutputMsg { port: "out".into(), msg_json: (n * 2.0).to_string() }])
//!     }
//! }
//! blocks_sdk::wasm::export_plugin!(MyPlugin);
//! ```
//!
//! # Unsafe
//!
//! The two raw-pointer slice reads below are the FFI boundary with
//! the host. The workspace policy is `unsafe_code = "forbid"`
//! elsewhere; here we `deny` at crate level and `allow` narrowly.
//!
//! **Build:** the block crate is a `cdylib` targeting
//! `wasm32-unknown-unknown`. One command:
//!
//! ```text
//! cargo build --target wasm32-unknown-unknown --release
//! ```
//!
//! The `.wasm` artifact is the block's `contributes.wasm_modules[].path`.

use serde::{Deserialize, Serialize};

/// Identity + declared kinds — returned from [`WasmPlugin::describe`].
/// Wire-equivalent to `blocks_host::wasm::DescribeResponse`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeResponse {
    pub extension_id: String,
    pub version: String,
    #[serde(default)]
    pub kinds: Vec<KindDecl>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KindDecl {
    pub kind_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub inputs: Vec<String>,
    #[serde(default)]
    pub outputs: Vec<String>,
}

/// Envelope handed to [`WasmPlugin::on_input`] — destructured from
/// the host's single-envelope JSON so authors see typed fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnInputEnvelope<'a> {
    pub node_id: &'a str,
    pub kind_id: &'a str,
    pub port: &'a str,
    pub msg_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputMsg {
    pub port: String,
    pub msg_json: String,
}

/// Implement this on a zero-sized marker type and wrap with
/// [`export_plugin!`].
pub trait WasmPlugin {
    fn describe() -> DescribeResponse;
    fn on_input(env: OnInputEnvelope<'_>) -> Result<Vec<OutputMsg>, String>;
}

// ---- ABI helpers exposed to the macro --------------------------------------

/// Pack `(ptr << 32) | len` into the i64 the host reads.
#[doc(hidden)]
#[inline]
pub fn pack(ptr: *const u8, len: usize) -> i64 {
    ((ptr as u64) << 32 | len as u64) as i64
}

/// Leak a `Vec<u8>` into a stable pointer. The host reads these bytes
/// out of linear memory and then drops the whole store, so nothing
/// needs to be freed guest-side.
#[doc(hidden)]
pub fn leak_bytes(bytes: Vec<u8>) -> (*const u8, usize) {
    let boxed = bytes.into_boxed_slice();
    let len = boxed.len();
    let ptr = Box::leak(boxed).as_ptr();
    (ptr, len)
}

/// Read a guest-owned byte slice the host wrote into linear memory
/// via the `alloc` / memory.write dance.
///
/// # Safety
/// Caller promises `ptr` and `len` came from the host's matching
/// `alloc` call in the same call frame.
#[doc(hidden)]
#[allow(unsafe_code)]
pub unsafe fn read_slice<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    std::slice::from_raw_parts(ptr, len)
}

/// Guest-side `alloc` the host calls before writing the envelope.
///
/// Authors never call this — [`export_plugin!`] re-exports it as the
/// `alloc` symbol the host imports.
#[doc(hidden)]
pub fn __sdk_alloc(size: i32) -> i32 {
    let mut v = Vec::<u8>::with_capacity(size as usize);
    let ptr = v.as_mut_ptr();
    std::mem::forget(v);
    ptr as i32
}

/// Shared guts of the exported `describe` entry point.
#[doc(hidden)]
pub fn __sdk_describe<P: WasmPlugin>() -> i64 {
    let resp = P::describe();
    let bytes = serde_json::to_vec(&resp).expect("DescribeResponse always serialises");
    let (ptr, len) = leak_bytes(bytes);
    pack(ptr, len)
}

/// Shared guts of the exported `on_input` entry point.
#[doc(hidden)]
pub fn __sdk_on_input<P: WasmPlugin>(env_ptr: i32, env_len: i32) -> i64 {
    // SAFETY: host promised these pointers in the current call.
    #[allow(unsafe_code)]
    let bytes = unsafe { read_slice(env_ptr as *const u8, env_len as usize) };
    let result: Result<Vec<OutputMsg>, String> = serde_json::from_slice(bytes)
        .map_err(|e| format!("malformed envelope: {e}"))
        .and_then(|env: OnInputEnvelope<'_>| P::on_input(env));
    let out = serde_json::to_vec(&result).expect("Result always serialises");
    let (ptr, len) = leak_bytes(out);
    pack(ptr, len)
}

/// Export the three ABI symbols (`alloc`, `describe`, `on_input`)
/// that the host links against.
///
/// Pass a type implementing [`WasmPlugin`]:
///
/// ```ignore
/// blocks_sdk::wasm::export_plugin!(MyPlugin);
/// ```
#[macro_export]
macro_rules! export_plugin {
    ($block:ty) => {
        #[no_mangle]
        pub extern "C" fn alloc(size: i32) -> i32 {
            $crate::wasm::__sdk_alloc(size)
        }

        #[no_mangle]
        pub extern "C" fn describe() -> i64 {
            $crate::wasm::__sdk_describe::<$block>()
        }

        #[no_mangle]
        pub extern "C" fn on_input(env_ptr: i32, env_len: i32) -> i64 {
            $crate::wasm::__sdk_on_input::<$block>(env_ptr, env_len)
        }
    };
}

pub use crate::export_plugin;
