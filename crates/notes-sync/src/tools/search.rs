//! Search tool for semantic search with graph boosting.

use anyhow::Result;
use once_cell::sync::Lazy;
use regex::Regex;
use rmcp::model::{CallToolResult, Content, ErrorData};
use std::path::Path;
use tokio::fs;

use crate::embeddings::EmbeddingManager;
use crate::graph::GraphIndex;
use semantic_embeddings::{EMBEDDING_DIM, Embedding};

/// Regex for extracting [[wiki-links]] from query text
static WIKI_LINK_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[\[([^\]]+)\]\]").expect("Invalid wiki-link regex"));

/// Regex for normalizing whitespace
static WHITESPACE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\s+").expect("Invalid whitespace regex"));

/// Hardcoded search parameters
const TOP_K: usize = 10;
const MIN_SIMILARITY: f32 = 0.3;

/// Search result with scores
#[derive(Debug)]
struct SearchResult {
    note_name: String,
    path: String,
    semantic_score: f32,
    graph_score: f32,
    final_score: f32,
}

/// Execute the Search tool.
pub async fn execute(
    vault_path: &Path,
    graph: &GraphIndex,
    embeddings: &EmbeddingManager,
    query: &str,
    debug: bool,
) -> Result<CallToolResult, ErrorData> {
    tracing::info!(query_len = query.len(), "Starting search");

    // Parse wiki-links from query
    let (note_refs, remaining_text) = parse_query(query);

    // Build the query embedding
    let query_embedding =
        build_query_embedding(vault_path, embeddings, &note_refs, &remaining_text)
            .await
            .map_err(|e| {
                ErrorData::internal_error(format!("Failed to build query embedding: {}", e), None)
            })?;

    // Get all note embeddings
    let notes = get_all_notes(vault_path, graph).await;
    if notes.is_empty() {
        return Ok(CallToolResult::success(vec![Content::text(
            "No notes found in vault.",
        )]));
    }

    tracing::info!(
        vault_notes = notes.len(),
        wiki_links = note_refs.len(),
        "Computing embeddings"
    );

    // Compute embeddings for all notes
    let note_embeddings = embeddings.get_embeddings_batch(&notes).await.map_err(|e| {
        ErrorData::internal_error(format!("Failed to compute embeddings: {}", e), None)
    })?;

    // Compute semantic similarity scores
    let mut results: Vec<SearchResult> = Vec::new();
    for (path, embedding) in &note_embeddings {
        let semantic_score =
            EmbeddingManager::cosine_similarity(&query_embedding, embedding).unwrap_or(0.0);

        if semantic_score < MIN_SIMILARITY {
            continue;
        }

        let note_name = Path::new(path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();

        // Compute graph proximity boost if we have note references
        let graph_score = if !note_refs.is_empty() {
            compute_graph_proximity(graph, &note_refs, &note_name)
        } else {
            0.0
        };

        // Apply multiplicative boost, capped at 100%
        let final_score = (semantic_score * (1.0 + graph_score)).min(1.0);

        results.push(SearchResult {
            note_name,
            path: path.clone(),
            semantic_score,
            graph_score,
            final_score,
        });
    }

    // Sort by final score descending
    results.sort_by(|a, b| b.final_score.partial_cmp(&a.final_score).unwrap());

    // Trim to top K
    results.truncate(TOP_K);

    tracing::info!(
        results = results.len(),
        top_score = results.first().map(|r| r.final_score).unwrap_or(0.0),
        "Search complete"
    );

    // Format output
    let output = format_results(&note_refs, &remaining_text, &results, debug);

    Ok(CallToolResult::success(vec![Content::text(output)]))
}

/// Parse wiki-links from query string.
fn parse_query(query: &str) -> (Vec<String>, String) {
    let mut note_refs = Vec::new();
    let mut remaining = query.to_string();

    // Find all [[wiki-links]] using pre-compiled regex
    for cap in WIKI_LINK_RE.captures_iter(query) {
        if let Some(m) = cap.get(1) {
            // Extract note name (handle aliases like [[Note|alias]])
            let note_ref = m.as_str();
            let note_name = note_ref.split('|').next().unwrap_or(note_ref);
            note_refs.push(note_name.to_string());
        }
    }

    // Remove wiki-links from remaining text
    remaining = WIKI_LINK_RE.replace_all(&remaining, "").to_string();
    remaining = remaining.trim().to_string();

    // Normalize whitespace using pre-compiled regex
    remaining = WHITESPACE_RE.replace_all(&remaining, " ").to_string();

    (note_refs, remaining)
}

/// Build query embedding from note references and remaining text.
async fn build_query_embedding(
    vault_path: &Path,
    embeddings: &EmbeddingManager,
    note_refs: &[String],
    remaining_text: &str,
) -> Result<Vec<f32>> {
    let mut texts = Vec::new();

    // Add content from referenced notes
    for note_ref in note_refs {
        // Try to find the note file
        let possible_paths = vec![
            vault_path.join(format!("{}.md", note_ref)),
            vault_path.join(format!("knowledge/{}.md", note_ref)),
            vault_path.join(format!("projects/{}.md", note_ref)),
            vault_path.join(format!("journal/{}.md", note_ref)),
        ];

        for path in possible_paths {
            if path.exists()
                && let Ok(content) = fs::read_to_string(&path).await
            {
                texts.push(content);
                break;
            }
        }
    }

    // Add remaining text if present
    if !remaining_text.is_empty() {
        texts.push(remaining_text.to_string());
    }

    // If no texts, use original query
    if texts.is_empty() {
        texts.push(note_refs.join(" ") + " " + remaining_text);
    }

    // Average embeddings for all texts
    let mut combined: Embedding = vec![0.0; EMBEDDING_DIM];
    let mut count = 0;

    for text in texts {
        let embedding = embeddings.get_embedding("__query__", &text).await?;
        for (i, val) in embedding.iter().enumerate() {
            combined[i] += val;
        }
        count += 1;
    }

    if count > 0 {
        for val in &mut combined {
            *val /= count as f32;
        }
    }

    Ok(combined)
}

/// Get all markdown notes in the vault.
async fn get_all_notes(vault_path: &Path, graph: &GraphIndex) -> Vec<(String, String)> {
    let mut notes = Vec::new();

    for rel_path in graph.all_paths() {
        let full_path = vault_path.join(rel_path);
        if let Ok(content) = fs::read_to_string(&full_path).await {
            notes.push((rel_path.clone(), content));
        }
    }

    notes
}

/// Compute graph proximity score using Personalized PageRank.
///
/// For single seed: returns PageRank score from that seed.
/// For multiple seeds: computes PageRank from each seed and multiplies scores
/// (intersection - note must be close to ALL seeds).
fn compute_graph_proximity(graph: &GraphIndex, seeds: &[String], target: &str) -> f32 {
    use crate::graph::pagerank::personalized_pagerank;

    if seeds.is_empty() {
        return 0.0;
    }

    let mut combined_score = 1.0;

    for seed in seeds {
        let scores = personalized_pagerank(graph, seed);
        let score = scores.get(target).copied().unwrap_or(0.0) as f32;

        // Multiply scores for intersection (must be close to ALL seeds)
        combined_score *= score;
    }

    // Cap at 1.0 (100% boost)
    combined_score.min(1.0)
}

/// Format search results for output.
fn format_results(
    note_refs: &[String],
    remaining_text: &str,
    results: &[SearchResult],
    debug: bool,
) -> String {
    let mut output = String::from("# Search Results\n\n");

    // Show what we're searching for
    if !note_refs.is_empty() || !remaining_text.is_empty() {
        output.push_str("Searching using: ");
        let parts: Vec<String> = note_refs
            .iter()
            .map(|n| format!("[[{}]]", n))
            .chain(if remaining_text.is_empty() {
                None
            } else {
                Some(format!("\"{}\"", remaining_text))
            })
            .collect();
        output.push_str(&parts.join(", "));
        output.push_str("\n\n");
    }

    if results.is_empty() {
        output.push_str("No relevant notes found.\n");
        return output;
    }

    output.push_str(&format!("Found {} relevant notes:\n\n", results.len()));

    for (i, result) in results.iter().enumerate() {
        let percent = (result.final_score * 100.0) as i32;
        output.push_str(&format!(
            "{}. **[[{}]]** ({}% relevant)\n",
            i + 1,
            result.note_name,
            percent
        ));

        if debug {
            let semantic_pct = (result.semantic_score * 100.0) as i32;
            let graph_pct = (result.graph_score * 100.0) as i32;

            output.push_str(&format!("   - Semantic: {}%\n", semantic_pct));
            output.push_str(&format!("   - Graph: {}%\n", graph_pct));

            // Show boost calculation
            if result.graph_score > 0.0 {
                let boosted = result.semantic_score * (1.0 + result.graph_score);
                if boosted > 1.0 {
                    output.push_str(&format!(
                        "   - Boost: {}% × {:.2} = {:.0}% (capped at 100%)\n",
                        semantic_pct,
                        1.0 + result.graph_score,
                        boosted * 100.0
                    ));
                } else {
                    output.push_str(&format!(
                        "   - Boost: {}% × {:.2} = {}%\n",
                        semantic_pct,
                        1.0 + result.graph_score,
                        percent
                    ));
                }
            }
        }

        output.push_str(&format!("   - Path: `{}`\n", result.path));
        output.push('\n');
    }

    output.push_str("*Use ReadNote to view note contents*\n");

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query_single_note() {
        let (refs, remaining) = parse_query("[[TypeScript]]");
        assert_eq!(refs, vec!["TypeScript"]);
        assert_eq!(remaining, "");
    }

    #[test]
    fn test_parse_query_multiple_notes() {
        let (refs, remaining) = parse_query("[[TypeScript]] [[Projects]]");
        assert_eq!(refs, vec!["TypeScript", "Projects"]);
        assert_eq!(remaining, "");
    }

    #[test]
    fn test_parse_query_mixed() {
        let (refs, remaining) = parse_query("type safety in [[TypeScript]]");
        assert_eq!(refs, vec!["TypeScript"]);
        assert_eq!(remaining, "type safety in");
    }

    #[test]
    fn test_parse_query_with_alias() {
        let (refs, remaining) = parse_query("[[Note Name|alias]]");
        assert_eq!(refs, vec!["Note Name"]);
        assert_eq!(remaining, "");
    }

    #[test]
    fn test_parse_query_plain_text() {
        let (refs, remaining) = parse_query("just plain text");
        assert!(refs.is_empty());
        assert_eq!(remaining, "just plain text");
    }
}
