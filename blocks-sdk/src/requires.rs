//! `requires!` — author-facing macro to declare a kind's capability
//! requirements.
//!
//! Emits a `requires()` function returning a `Vec<Requirement>` that
//! the host's installer matches against its capability registry per
//! `docs/design/VERSIONING.md`. A `const REQUIRES: &[Requirement]`
//! would be nicer but `SemverRange::parse` isn't `const` — keep it as a
//! call so `requires!` stays a one-liner with no `unwrap_or` ceremony.
//!
//! ```ignore
//! use blocks_sdk::prelude::*;
//!
//! requires! {
//!     "spi.msg" => "1",
//! }
//! ```

#[macro_export]
macro_rules! requires {
    ( $( $cap:literal => $range:literal ),* $(,)? ) => {
        pub fn requires() -> ::std::vec::Vec<$crate::capabilities::Requirement> {
            ::std::vec![
                $( $crate::capabilities::Requirement::required(
                    $crate::capabilities::CapabilityId::new($cap),
                    $crate::capabilities::SemverRange::caret($range)
                        .expect("requires!: invalid semver range literal"),
                ) ),*
            ]
        }
    };
}
