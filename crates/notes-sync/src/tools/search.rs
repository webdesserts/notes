//! Search and recommend tools: semantic search with graph boosting.

use anyhow::Result;
use once_cell::sync::Lazy;
use regex::Regex;
use rmcp::model::{CallToolResult, Content, ErrorData};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tokio::fs;

use crate::embeddings::EmbeddingManager;
use crate::graph::GraphIndex;
use crate::graph::pagerank::{personalized_pagerank, resolve_note_ref};
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

/// Weight of the graph score in recommend's additive score
const GRAPH_WEIGHT: f32 = 0.3;
/// Number of top graph-proximity notes recommend considers besides the semantic matches
const GRAPH_CANDIDATES: usize = 20;
const DEFAULT_RECOMMEND_LIMIT: usize = 5;
const MAX_RECOMMEND_LIMIT: usize = 20;

/// Search result with scores
#[derive(Debug)]
struct SearchResult {
    note_name: String,
    path: String,
    semantic_score: f32,
    graph_score: f32,
    final_score: f32,
}

/// One recommended note, as returned in recommend's structured content.
#[derive(Debug, Serialize, PartialEq)]
pub struct Recommendation {
    pub path: String,
    pub name: String,
    pub score: f32,
    pub semantic: f32,
    pub graph: f32,
}

/// Everything a query needs, computed once per call.
struct QueryContext {
    /// Vault-relative paths the note references resolved to
    seed_paths: Vec<String>,
    /// (path, embedding) for every note in the vault
    note_embeddings: Vec<(String, Vec<f32>)>,
    /// None when there is neither text nor a resolvable reference
    query_embedding: Option<Embedding>,
    /// Personalized PageRank from all seeds at once, keyed by path
    graph_scores: HashMap<String, f64>,
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

    let (note_refs, remaining_text) = parse_query(query);
    let context = prepare(vault_path, graph, embeddings, &note_refs, &remaining_text).await?;

    if context.note_embeddings.is_empty() {
        return Ok(CallToolResult::success(vec![Content::text(
            "No notes found in vault.",
        )]));
    }

    let mut results: Vec<SearchResult> = Vec::new();
    if let Some(query_embedding) = &context.query_embedding {
        for (path, embedding) in &context.note_embeddings {
            let semantic_score =
                EmbeddingManager::cosine_similarity(query_embedding, embedding).unwrap_or(0.0);

            if semantic_score < MIN_SIMILARITY {
                continue;
            }

            let graph_score = context.graph_scores.get(path).copied().unwrap_or(0.0) as f32;

            // Apply multiplicative boost, capped at 100%
            let final_score = (semantic_score * (1.0 + graph_score)).min(1.0);

            results.push(SearchResult {
                note_name: note_name(path),
                path: path.clone(),
                semantic_score,
                graph_score,
                final_score,
            });
        }
    }

    // Sort by final score descending
    results.sort_by(|a, b| b.final_score.total_cmp(&a.final_score));

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

/// Execute the Recommend tool.
///
/// Seeds come from `seeds` plus any [[wiki-links]] in `query`. Candidates are the
/// notes with semantic similarity >= MIN_SIMILARITY plus the top GRAPH_CANDIDATES
/// by graph proximity; score = semantic + GRAPH_WEIGHT * graph (not capped).
/// Seed notes and `exclude` (paths or names) never appear in the results.
pub async fn recommend(
    vault_path: &Path,
    graph: &GraphIndex,
    embeddings: &EmbeddingManager,
    query: &str,
    seeds: &[String],
    exclude: &[String],
    limit: Option<usize>,
) -> Result<CallToolResult, ErrorData> {
    let (mut note_refs, remaining_text) = parse_query(query);
    for seed in seeds {
        if !note_refs.contains(seed) {
            note_refs.push(seed.clone());
        }
    }
    let limit = limit
        .unwrap_or(DEFAULT_RECOMMEND_LIMIT)
        .clamp(1, MAX_RECOMMEND_LIMIT);

    tracing::info!(
        query_len = remaining_text.len(),
        seeds = note_refs.len(),
        exclude = exclude.len(),
        "Starting recommend"
    );

    let context = prepare(vault_path, graph, embeddings, &note_refs, &remaining_text).await?;

    let mut excluded: HashSet<String> = context.seed_paths.iter().cloned().collect();
    for note_ref in exclude {
        excluded.extend(resolve_note_ref(graph, note_ref));
    }

    let semantic: Vec<(String, f32)> = match &context.query_embedding {
        Some(query_embedding) => context
            .note_embeddings
            .iter()
            .map(|(path, embedding)| {
                let score =
                    EmbeddingManager::cosine_similarity(query_embedding, embedding).unwrap_or(0.0);
                (path.clone(), score)
            })
            .collect(),
        None => Vec::new(),
    };

    let results = rank_recommendations(&semantic, &context.graph_scores, &excluded, limit);

    tracing::info!(results = results.len(), "Recommend complete");

    Ok(CallToolResult::structured(serde_json::json!({
        "query": remaining_text,
        "seeds": context.seed_paths,
        "results": results,
    })))
}

/// Rank recommend candidates: (semantic >= MIN_SIMILARITY) ∪ (top GRAPH_CANDIDATES by graph).
fn rank_recommendations(
    semantic: &[(String, f32)],
    graph_scores: &HashMap<String, f64>,
    excluded: &HashSet<String>,
    limit: usize,
) -> Vec<Recommendation> {
    let semantic_by_path: HashMap<&str, f32> = semantic
        .iter()
        .map(|(path, score)| (path.as_str(), *score))
        .collect();

    let mut candidates: HashSet<&str> = semantic
        .iter()
        .filter(|(_, score)| *score >= MIN_SIMILARITY)
        .map(|(path, _)| path.as_str())
        .collect();

    let mut by_graph: Vec<(&str, f64)> = graph_scores
        .iter()
        .filter(|(path, _)| !excluded.contains(path.as_str()))
        .map(|(path, score)| (path.as_str(), *score))
        .collect();
    by_graph.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    candidates.extend(
        by_graph
            .into_iter()
            .take(GRAPH_CANDIDATES)
            .map(|(path, _)| path),
    );

    let mut results: Vec<Recommendation> = candidates
        .into_iter()
        .filter(|path| !excluded.contains(*path))
        .map(|path| {
            let semantic = semantic_by_path.get(path).copied().unwrap_or(0.0);
            let graph = graph_scores.get(path).copied().unwrap_or(0.0) as f32;
            Recommendation {
                path: path.to_string(),
                name: note_name(path),
                score: semantic + GRAPH_WEIGHT * graph,
                semantic,
                graph,
            }
        })
        .collect();

    results.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.path.cmp(&b.path))
    });
    results.truncate(limit);
    results
}

/// Resolve seeds, embed the vault and the query, and run PageRank once.
async fn prepare(
    vault_path: &Path,
    graph: &GraphIndex,
    embeddings: &EmbeddingManager,
    note_refs: &[String],
    remaining_text: &str,
) -> Result<QueryContext, ErrorData> {
    let mut seed_paths: Vec<String> = Vec::new();
    for note_ref in note_refs {
        for path in resolve_note_ref(graph, note_ref) {
            if !seed_paths.contains(&path) {
                seed_paths.push(path);
            }
        }
    }

    let notes = get_all_notes(vault_path, graph).await;

    tracing::info!(
        vault_notes = notes.len(),
        wiki_links = note_refs.len(),
        seeds = seed_paths.len(),
        "Computing embeddings"
    );

    let note_embeddings = if notes.is_empty() {
        Vec::new()
    } else {
        embeddings.get_embeddings_batch(&notes).await.map_err(|e| {
            ErrorData::internal_error(format!("Failed to compute embeddings: {}", e), None)
        })?
    };

    let query_embedding = build_query_embedding(
        embeddings,
        &note_embeddings,
        &seed_paths,
        note_refs,
        remaining_text,
    )
    .await
    .map_err(|e| {
        ErrorData::internal_error(format!("Failed to build query embedding: {}", e), None)
    })?;

    let graph_scores = if seed_paths.is_empty() {
        HashMap::new()
    } else {
        personalized_pagerank(graph, &seed_paths)
    };

    Ok(QueryContext {
        seed_paths,
        note_embeddings,
        query_embedding,
        graph_scores,
    })
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

/// Build the query embedding: the average of the seed notes' cached embeddings
/// and the embedding of the remaining text.
///
/// Seed embeddings come straight from the vault batch (keyed by path), so seeds
/// are never re-read or re-embedded. If no reference resolved and there is no
/// text, the reference names themselves are embedded.
async fn build_query_embedding(
    embeddings: &EmbeddingManager,
    note_embeddings: &[(String, Vec<f32>)],
    seed_paths: &[String],
    note_refs: &[String],
    remaining_text: &str,
) -> Result<Option<Embedding>> {
    let mut vectors: Vec<Vec<f32>> = note_embeddings
        .iter()
        .filter(|(path, _)| seed_paths.contains(path))
        .map(|(_, embedding)| embedding.clone())
        .collect();

    if !remaining_text.is_empty() {
        vectors.push(
            embeddings
                .get_embedding("__query__", remaining_text)
                .await?,
        );
    }

    if vectors.is_empty() && !note_refs.is_empty() {
        vectors.push(
            embeddings
                .get_embedding("__query__", &note_refs.join(" "))
                .await?,
        );
    }

    Ok(average(&vectors))
}

/// Element-wise mean of embeddings, None for an empty list.
fn average(vectors: &[Vec<f32>]) -> Option<Embedding> {
    if vectors.is_empty() {
        return None;
    }
    let mut combined: Embedding = vec![0.0; EMBEDDING_DIM];
    for vector in vectors {
        for (sum, val) in combined.iter_mut().zip(vector) {
            *sum += val;
        }
    }
    for val in &mut combined {
        *val /= vectors.len() as f32;
    }
    Some(combined)
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

/// Note name (file stem) of a vault-relative path.
fn note_name(path: &str) -> String {
    Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string()
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

    fn semantic(entries: &[(&str, f32)]) -> Vec<(String, f32)> {
        entries.iter().map(|(p, s)| (p.to_string(), *s)).collect()
    }

    fn graph(entries: &[(&str, f64)]) -> HashMap<String, f64> {
        entries.iter().map(|(p, s)| (p.to_string(), *s)).collect()
    }

    #[test]
    fn test_rank_adds_graph_only_candidates() {
        let sem = semantic(&[("a.md", 0.6), ("b.md", 0.1)]);
        let gr = graph(&[("b.md", 1.0)]);
        let results = rank_recommendations(&sem, &gr, &HashSet::new(), 5);

        let paths: Vec<&str> = results.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["a.md", "b.md"]);
        assert!((results[1].score - (0.1 + GRAPH_WEIGHT)).abs() < 1e-6);
        assert_eq!(results[1].name, "b");
    }

    #[test]
    fn test_rank_scores_are_uncapped_and_ordered() {
        let sem = semantic(&[("x/a.md", 0.9), ("b.md", 0.95)]);
        let gr = graph(&[("x/a.md", 1.0), ("b.md", 0.1)]);
        let results = rank_recommendations(&sem, &gr, &HashSet::new(), 5);

        assert_eq!(results[0].path, "x/a.md");
        assert!(results[0].score > 1.0);
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn test_rank_excludes_and_limits() {
        let sem = semantic(&[("a.md", 0.9), ("b.md", 0.8), ("c.md", 0.7), ("d.md", 0.2)]);
        let gr = graph(&[("seed.md", 1.0), ("a.md", 0.5)]);
        let excluded: HashSet<String> = ["seed.md", "a.md"].iter().map(|s| s.to_string()).collect();
        let results = rank_recommendations(&sem, &gr, &excluded, 2);

        let paths: Vec<&str> = results.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["b.md", "c.md"]);
    }

    #[test]
    fn test_average() {
        assert!(average(&[]).is_none());
        let mut a = vec![0.0; EMBEDDING_DIM];
        let mut b = vec![0.0; EMBEDDING_DIM];
        a[0] = 1.0;
        b[0] = 3.0;
        let avg = average(&[a, b]).unwrap();
        assert!((avg[0] - 2.0).abs() < 1e-6);
    }
}
