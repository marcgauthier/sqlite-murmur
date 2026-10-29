//! Internal subset of ipnet 2.12.2 for CIDR parsing, address containment, and Serde.
//! The package name/version are retained so Cargo can patch transitive consumers.
//! See PROVENANCE.md and the retained upstream licenses for attribution and scope.
extern crate alloc;

mod ipnet;
#[cfg(feature = "serde")]
mod ipnet_serde;
mod parser;

pub use ipnet::{IpNet, Ipv4Net, Ipv6Net, PrefixLenError};
pub use parser::AddrParseError;
