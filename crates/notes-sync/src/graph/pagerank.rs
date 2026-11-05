//! Personalized PageRank implementation for graph proximity scoring.

use super::GraphIndex;
use std::collections::HashMap;
use std::path::Path;

const DEFAULT_DAMPING: f64 = 0.85;
const DEFAULT_TOLERANCE: f64 = 1e-6;
const DEFAULT_MAX_ITER: usize = 100;

/// Compute Personalized PageRank scores from a seed note.
///
/// Returns a map of note names to proximity scores (0.0-1.0), where higher
/// scores indicate stronger connection to the seed through the link graph.
///
/// Algorithm:
/// - Random walks start at the seed node
/// - At each step: 85% follow random link, 15% restart at seed
/// - Iterate until convergence or max iterations
pub fn personalized_pagerank(graph: &GraphIndex, seed: &str) -> HashMap<String, f64> {
    personalized_pagerank_with_params(
        graph,
        seed,
        DEFAULT_DAMPING,
        DEFAULT_TOLERANCE,
        DEFAULT_MAX_ITER,
    )
}

/// Compute Personalized PageRank with custom parameters.
pub fn personalized_pagerank_with_params(
    graph: &GraphIndex,
    seed: &str,
    damping: f64,
    tolerance: f64,
    max_iter: usize,
) -> HashMap<String, f64> {
    // Build list of all nodes (paths)
    let nodes: Vec<String> = graph.all_paths().cloned().collect();
    let n = nodes.len();

    // Seed is a note name, find matching paths
    let seed_paths: Vec<&String> = nodes
        .iter()
        .filter(|p| Path::new(p).file_stem().and_then(|s| s.to_str()) == Some(seed))
        .collect();

    if n == 0 || seed_paths.is_empty() {
        return HashMap::new();
    }

    // Initialize scores uniformly
    let mut scores: HashMap<String, f64> = HashMap::new();
    let init_score = 1.0 / n as f64;
    for node in &nodes {
        scores.insert(node.clone(), init_score);
    }

    // Personalization vector (restart only at seed paths)
    let mut personalization: HashMap<String, f64> = HashMap::new();
    let restart_prob = 1.0 / seed_paths.len() as f64;
    for seed_path in &seed_paths {
        personalization.insert((*seed_path).clone(), restart_prob);
    }

    // Pre-compute reverse adjacency map (node -> nodes that link to it)
    // This changes algorithm from O(n²) to O(n×avg_degree) per iteration
    let start_time = std::time::Instant::now();
    let mut incoming: HashMap<String, Vec<String>> = HashMap::new();
    for source_node in &nodes {
        let neighbors = graph.get_neighborhood(source_node);
        for target in neighbors {
            incoming
                .entry(target)
                .or_default()
                .push(source_node.clone());
        }
    }

    tracing::debug!(
        seed = seed,
        nodes = n,
        avg_incoming = incoming.values().map(|v| v.len()).sum::<usize>() / n.max(1),
        "Starting Personalized PageRank"
    );

    // Iteratively compute PageRank
    let mut converged_at = None;
    for iteration in 0..max_iter {
        let mut new_scores: HashMap<String, f64> = HashMap::new();

        for node in &nodes {
            let mut score = 0.0;

            // Sum contributions from nodes that link to this node (reverse adjacency)
            if let Some(sources) = incoming.get(node) {
                for source_node in sources {
                    let source_score = scores.get(source_node).copied().unwrap_or(0.0);
                    let source_out_degree = graph.get_neighborhood(source_node).len() as f64;

                    if source_out_degree > 0.0 {
                        score += source_score / source_out_degree;
                    }
                }
            }

            // Apply damping and personalization
            let restart_prob = personalization.get(node).copied().unwrap_or(0.0);
            score = damping * score + (1.0 - damping) * restart_prob;

            new_scores.insert(node.clone(), score);
        }

        // Check convergence (L1 distance)
        let mut diff = 0.0;
        for node in &nodes {
            let old = scores.get(node).copied().unwrap_or(0.0);
            let new = new_scores.get(node).copied().unwrap_or(0.0);
            diff += (new - old).abs();
        }

        scores = new_scores;

        // Log progress every 10 iterations
        if iteration % 10 == 0 && iteration > 0 {
            tracing::debug!(
                iteration = iteration,
                max_iter = max_iter,
                diff = diff,
                "PageRank iteration progress"
            );
        }

        if diff < tolerance {
            converged_at = Some(iteration);
            break;
        }
    }

    let elapsed = start_time.elapsed();
    if let Some(iter) = converged_at {
        tracing::debug!(
            seed = seed,
            iterations = iter,
            elapsed_ms = elapsed.as_millis(),
            "PageRank converged"
        );
    } else {
        tracing::warn!(
            seed = seed,
            max_iter = max_iter,
            elapsed_ms = elapsed.as_millis(),
            "PageRank did not converge"
        );
    }

    // Normalize so all scores are relative to the max score
    // This ensures the seed (or most connected node) has score ~1.0
    let max_score = scores.values().copied().fold(0.0f64, f64::max);
    if max_score > 0.0 {
        for score in scores.values_mut() {
            *score /= max_score;
        }
    }

    // Convert path keys to note name keys for the caller
    // If multiple paths have the same note name, take the max score
    let mut name_scores: HashMap<String, f64> = HashMap::new();
    for (path, score) in scores {
        if let Some(name) = Path::new(&path).file_stem().and_then(|s| s.to_str()) {
            let entry = name_scores.entry(name.to_string()).or_insert(0.0);
            *entry = entry.max(score);
        }
    }

    name_scores
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::path::PathBuf;

    #[test]
    fn test_linear_graph() {
        // A -> B -> C (bidirectional due to backlinks)
        let mut graph = GraphIndex::new();

        let mut links_a = HashSet::new();
        links_a.insert("B".to_string());
        graph.update_note("A", PathBuf::from("A.md"), links_a);

        let mut links_b = HashSet::new();
        links_b.insert("C".to_string());
        graph.update_note("B", PathBuf::from("B.md"), links_b);

        graph.update_note("C", PathBuf::from("C.md"), HashSet::new());

        let scores = personalized_pagerank(&graph, "A");

        // Get scores
        let score_a = scores.get("A").copied().unwrap_or(0.0);
        let score_b = scores.get("B").copied().unwrap_or(0.0);
        let score_c = scores.get("C").copied().unwrap_or(0.0);

        // B is directly connected to A (1-hop), C is 2-hops away
        // A should have highest score (normalized to 1.0)
        // B should be close to A since it's directly connected
        // C should have lower score (further away)
        assert!(score_a >= 0.7); // Seed has high score
        assert!(score_b >= 0.5); // Direct neighbor
        assert!(score_c < score_b); // Further away has lower score
        assert!(score_c > 0.0); // But still reachable
    }

    #[test]
    fn test_disconnected_nodes() {
        // A -> B, C (disconnected)
        let mut graph = GraphIndex::new();

        let mut links_a = HashSet::new();
        links_a.insert("B".to_string());
        graph.update_note("A", PathBuf::from("A.md"), links_a);

        graph.update_note("B", PathBuf::from("B.md"), HashSet::new());
        graph.update_note("C", PathBuf::from("C.md"), HashSet::new());

        let scores = personalized_pagerank(&graph, "A");

        let score_b = scores.get("B").copied().unwrap_or(0.0);
        let score_c = scores.get("C").copied().unwrap_or(0.0);

        // B should have higher score than disconnected C
        assert!(score_b > score_c);
    }
}
