//! Length-prefixed framing primitives for QUIC bi-streams.
//!
//! These helpers frame raw byte payloads on an iroh bi-stream using a simple
//! `[u32 little-endian length][bytes]` envelope. They are protocol-agnostic —
//! the pairing handler (`PAIRING_ALPN`) uses them, and the daemon's pumped
//! `SYNC_ALPN` path re-derives the same envelope — so they live in p2p-core, the
//! shared networking substrate.

use anyhow::Result;
use iroh::endpoint::{RecvStream, SendStream};

/// Maximum byte length for a framed message.
///
/// 64 MiB covers a realistic worst case of syncing a large vault in one batch.
/// Messages larger than this are rejected to prevent memory exhaustion.
pub const MAX_MESSAGE_BYTES: u32 = 64 * 1024 * 1024;

/// Write a length-prefixed byte slice to the stream.
///
/// Format: `[u32 little-endian length][bytes]`
pub async fn write_length_prefixed(send: &mut SendStream, bytes: &[u8]) -> Result<()> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| anyhow::anyhow!("Message too large to frame: {} bytes", bytes.len()))?;
    send.write_all(&len.to_le_bytes()).await?;
    send.write_all(bytes).await?;
    Ok(())
}

/// Read a length-prefixed byte slice from the stream.
///
/// Format: `[u32 little-endian length][bytes]`
pub async fn read_length_prefixed(recv: &mut RecvStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf);

    if len > MAX_MESSAGE_BYTES {
        return Err(anyhow::anyhow!(
            "Sync message too large: {} bytes (max {})",
            len,
            MAX_MESSAGE_BYTES
        ));
    }

    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf).await?;
    Ok(buf)
}
