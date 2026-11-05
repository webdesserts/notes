//! The relay-address value type.
//!
//! `RelayAddr` is p2p-core's wrapper over iroh's `RelayUrl`, carrying the exact
//! same parsed-and-validated value. It exists so consumers (vault-sync, the
//! daemon) speak p2p-core's vocabulary at the API boundary instead of naming
//! iroh directly. The wrapped value is byte-identical to the iroh type, so
//! nothing on the wire or on disk changes.

use std::fmt;
use std::str::FromStr;

use iroh::RelayUrl;
use thiserror::Error;

/// Error parsing a string into a [`RelayAddr`].
#[derive(Debug, Error)]
pub enum RelayAddrError {
    #[error("invalid relay URL: {0}")]
    Invalid(String),
}

/// A parsed-and-validated relay URL (e.g. `https://umbra.computer/`).
///
/// Wraps iroh's `RelayUrl` (same bytes/value) so consumers speak p2p-core's
/// vocabulary instead of naming iroh directly. **Not serialized** — `daemon.toml`
/// stores relay URLs as plain `String`; parse to `RelayAddr` after load and
/// format back via [`RelayAddr::as_str`] (full URL) before save.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelayAddr(RelayUrl);

impl RelayAddr {
    /// Parse from a URL string. Equivalent to parsing the underlying iroh
    /// `RelayUrl`, so the round-trip through `as_str()` is lossless for a valid
    /// relay URL.
    pub fn parse(s: &str) -> Result<Self, RelayAddrError> {
        s.parse::<RelayUrl>()
            .map(RelayAddr)
            .map_err(|e| RelayAddrError::Invalid(e.to_string()))
    }

    /// The full URL string (round-trips to `daemon.toml` / logs). Byte-identical
    /// to the iroh `RelayUrl`'s string form.
    pub fn as_str(&self) -> String {
        self.0.to_string()
    }

    /// The host portion only (replaces the daemon's host-only `relay_display`).
    pub fn host(&self) -> Option<String> {
        self.0.host_str().map(str::to_owned)
    }

    /// The port portion, if present.
    pub fn port(&self) -> Option<u16> {
        self.0.port()
    }

    /// p2p-core-internal: hand the wrapped iroh value to iroh APIs.
    pub(crate) fn as_iroh(&self) -> &RelayUrl {
        &self.0
    }

    /// p2p-core-internal: wrap an iroh value produced inside p2p-core (e.g.
    /// `EmbeddedRelay`'s bound URL, peer-conn classification).
    pub(crate) fn from_iroh(url: RelayUrl) -> Self {
        RelayAddr(url)
    }
}

impl FromStr for RelayAddr {
    type Err = RelayAddrError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Host-only `Display`, matching the daemon's former `relay_display`. Sites that
/// need the FULL URL use [`RelayAddr::as_str`] instead, so log output stays
/// byte-identical to before the wrapper was introduced.
impl fmt::Display for RelayAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.host_str() {
            Some(h) => write!(f, "{h}"),
            None => write!(f, "{}", self.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The load-bearing claim: `RelayAddr` carries the same bytes as the iroh
    /// `RelayUrl`, so a string round-trip through it is verbatim. This is what
    /// keeps `daemon.toml` and log output byte-stable across the wrapper.
    #[test]
    fn relay_addr_round_trips_string_identically() {
        for s in [
            "https://umbra.computer/",
            "http://192.168.68.59:3340/",
            "http://example.com:3340/",
        ] {
            let addr = RelayAddr::parse(s).unwrap();
            assert_eq!(addr.as_str(), s); // round-trips verbatim
            let reparsed = RelayAddr::parse(&addr.as_str()).unwrap();
            assert_eq!(addr, reparsed); // idempotent
        }
    }

    #[test]
    fn host_and_port_accessors() {
        let addr = RelayAddr::parse("http://192.168.68.59:3340/").unwrap();
        assert_eq!(addr.host().as_deref(), Some("192.168.68.59"));
        assert_eq!(addr.port(), Some(3340));
    }

    #[test]
    fn display_is_host_only() {
        let addr = RelayAddr::parse("https://umbra.computer/").unwrap();
        assert_eq!(format!("{addr}"), "umbra.computer");
    }
}
