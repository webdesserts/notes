# p2p-core

The native peer-to-peer networking substrate wrapping [iroh] — peer identity, pairing,
mDNS discovery, gossip, an embedded relay, and the node that ties them together.

p2p-core owns the **application-agnostic** half of the mesh: everything that isn't a
specific app's wire protocol. A host crate (today, `vault-sync`) depends on p2p-core and
layers its own protocol on top via the ALPN seam (below). It was extracted from
obsidian-memory's sync daemon across a six-increment refactor so the iroh bootstrap can
be reused by other webdesserts p2p apps.

It is **native-only** — no `cfg(target_arch = "wasm32")` branches, no `native` feature
gate. It simply *is* the native networking crate.

## Architecture

**Acyclic dependency inversion.** The rule is one-directional: `vault-sync → p2p-core`,
never the reverse. p2p-core's `Cargo.toml` must never name a consumer crate. It speaks in
generic primitives (`u64` topic ids, `PeerId`, `TopicId`) and lets the host map its own
identities onto them (e.g. `topic_from_u64` / `u64_from_topic`).

**The ALPN seam.** `P2pNode` owns the iroh endpoint and protocol router. It registers
`GOSSIP_ALPN` and `PAIRING_ALPN` itself, and additionally accepts a host-supplied **sync
ALPN + `ProtocolHandler`** — that handler is the seam. The host app hands p2p-core its
protocol's ALPN and handler at construction; p2p-core wires it into the router alongside
gossip and pairing. The host never re-implements the node; it layers on via extension
traits (see `vault_sync::network`'s `SyncNodeSeam` / `VaultGossipExt`).

## What's in the box

| Area | Public surface |
|------|----------------|
| **Identity** | `PeerId`, `IdentityKey`, `FileKeyStorage` / `KeyStorage` — ed25519 device identity + on-disk key |
| **Node** | `P2pNode` (iroh endpoint + protocol router), `topic_from_u64` / `u64_from_topic`, gossip `join_topic`, `GOSSIP_ALPN` |
| **Pairing** | `pairing` (hello → challenge → response, HMAC-SHA256, `generate_pairing_code`) + `pairing_handler` (the QUIC handler, `PAIRING_ALPN`, `pair_with_mesh`) |
| **Allowlist** | `AllowlistStorage` trait + `InMemoryAllowlist` / `FileAllowlistStorage`, `AllowedPeer`, `write_pair_allowlist` — the authz roster pairing populates |
| **Discovery** | `mesh_mdns` (the mDNS actor, `MeshMdns`) + `discovery` types (`MeshMetadata`, `DiscoveryEvent`, …) |
| **Relay** | `EmbeddedRelay` (a self-hostable iroh relay server) + `relay_is_offlan_reachable` (the off-LAN / Tier-2 classifier) |
| **Config** | `DaemonConfig` / `PeerRelay` / `persist_config_change` (the `.sync/daemon.toml` substrate) + `DaemonLock` (the `.sync/daemon.lock` single-instance guard) |
| **Streams** | length-prefixed message framing (`read_length_prefixed` / `write_length_prefixed`, `MAX_MESSAGE_BYTES`) shared across protocols |
| **time_scale** | a process-wide time-scaling knob so timing-dependent tests run deterministically |

## Usage sketch

A host app builds a `P2pNode` with its own sync ALPN + handler, joins a topic, and uses
pairing + the allowlist for authentication:

```rust
// The host supplies SYNC_ALPN + a ProtocolHandler; p2p-core registers it alongside
// gossip + pairing and returns the node (plus an inbound receiver, for the default
// handler path).
let node = P2pNode::with_sync_alpn(secret_key, &relays, allowlist, SYNC_ALPN, handler).await?;
let topic = node.join_topic(topic_from_u64(my_id), bootstrap_peers).await?;
```

`vault-sync` is the reference consumer — see its `network` module for the seam in practice.

## The `test-util` feature

Exposes relay-only constructors (endpoints with no IP transports) so downstream
integration-test crates can reproduce off-LAN / NAT topologies. **Never enabled in
production.** It's a Cargo feature rather than `#[cfg(test)]` because `#[cfg(test)]`
does not cross the crate boundary into a consumer's integration tests.

## Contracts that cross versions (don't change casually)

- **ALPN strings** (`obsidian-memory/sync/1`, `obsidian-memory/pair/1`, the gossip ALPN)
  are wire contracts between fleet peers. (Re-namespacing away from the `obsidian-memory/`
  prefix is a deferred, coordinated flag-day — not a tidy-up.)
- **`daemon.toml`** is live config on deployed fleets; its on-disk shape is pinned by a
  byte-stability test (`tests/config_format.rs`).
- **Pairing message structs** are bincode wire contracts (byte-stability tested).

## Status

Currently the networking substrate for the obsidian-memory sync stack. The broader
direction — a shared multi-app mesh platform with pluggable pairing-based auth and
protocol version-gating — is the roadmap, not yet fully realized. Some naming is still
obsidian-memory-specific (the ALPN prefix, the `obsidian-sync` mDNS service) pending a
coordinated re-namespacing.

[iroh]: https://github.com/n0-computer/iroh
