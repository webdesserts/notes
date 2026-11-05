//! Semantic embeddings management for the MCP server.
//!
//! This module handles:
//! - Model downloading from Hugging Face at runtime (default)
//! - Model loading from embedded binary (with `embedded-model` feature)
//! - Embedding generation with caching
//! - Cache persistence to disk

#[cfg(not(feature = "embedded-model"))]
mod download;
mod manager;

pub use manager::EmbeddingManager;
