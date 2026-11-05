//! Semantic embeddings management for the MCP server.
//!
//! This module handles:
//! - Model downloading from Hugging Face at runtime (default)
//! - Model loading from embedded binary (with `embedded-model` feature)
//! - Embedding generation with caching
//! - Cache persistence to a versioned, atomically-replaced snapshot under
//!   `.obsidian/`, reused across processes when content and model identity match

#[cfg(not(feature = "embedded-model"))]
mod download;
mod manager;
mod persist;

pub use manager::EmbeddingManager;
