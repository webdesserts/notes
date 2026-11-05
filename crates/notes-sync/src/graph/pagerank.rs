//! Personalized PageRank implementation for graph proximity scoring.

use super::GraphIndex;
use std::collections::HashMap;

const DEFAULT_DAMPING: f64 = 0.85;
const DEFAULT_TOLERANCE: f64 = 1e-6;
const DEFAULT_MAX_ITER: usize = 100;

/// Resolve a note reference to vault-relative paths.
///
/// Accepts a relative path with or without `.md` (`projects/X`, `projects/X.md`)
/// or a bare note name (`X`), which matches every same-named note in any folder.
/// A `#heading` or `|alias` suffix is ignored. A path that does not exist resolves
/// to nothing (it does not fall back to a name lookup).
pub fn resolve_note_ref(graph: &GraphIndex, note_ref: &str) -> Vec<String> {
    let base = note_ref.split('|').next().unwrap_or_default();
    let base = base.split('#').next().unwrap_or_default();
    let trimmed = base.trim().trim_start_matches('/');
    let stem = trimmed.strip_suffix(".md").unwrap_or(trimmed);
    if stem.is_empty() {
        return Vec::new();
    }

    let with_ext = format!("{stem}.md");
    if graph.get_forward_links(&with_ext).is_some() {
        return vec![with_ext];
    }
    if stem.contains('/') {
        return Vec::new();
    }

    let mut paths: Vec<String> = graph
        .get_paths_for_name(stem)
        .map(|paths| paths.iter().cloned().collect())
        .unwrap_or_default();
    paths.sort();
    paths
}

/// Compute Personalized PageRank scores from one or more seed paths.
///
/// Returns a map of vault-relative paths to proximity scores (0.0-1.0, normalized
/// to the max), where higher scores indicate stronger connection to the seeds
/// through the link graph. Paths with zero score are omitted.
///
/// Algorithm:
/// - Random walks start at the seed nodes (restart probability split evenly)
/// - At each step: 85% follow random link, 15% restart at a seed
/// - Iterate until convergence or max iterations
pub fn personalized_pagerank(graph: &GraphIndex, seed_paths: &[String]) -> HashMap<String, f64> {
    personalized_pagerank_with_params(
        graph,
        seed_paths,
        DEFAULT_DAMPING,
        DEFAULT_TOLERANCE,
        DEFAULT_MAX_ITER,
    )
}

/// Compute Personalized PageRank with custom parameters.
pub fn personalized_pagerank_with_params(
    graph: &GraphIndex,
    seed_paths: &[String],
    damping: f64,
    tolerance: f64,
    max_iter: usize,
) -> HashMap<String, f64> {
    // Index nodes once so the iteration works on dense vectors
    let mut nodes: Vec<&String> = graph.all_paths().collect();
    nodes.sort();
    let n = nodes.len();
    let index: HashMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, path)| (path.as_str(), i))
        .collect();

    let mut seeds: Vec<usize> = seed_paths
        .iter()
        .filter_map(|path| index.get(path.as_str()).copied())
        .collect();
    seeds.sort_unstable();
    seeds.dedup();

    if n == 0 || seeds.is_empty() {
        return HashMap::new();
    }

    // Personalization vector (restart only at seed paths)
    let mut personalization = vec![0.0; n];
    let restart_prob = 1.0 / seeds.len() as f64;
    for &seed in &seeds {
        personalization[seed] = restart_prob;
    }

    // Reverse adjacency and out-degrees, computed once per call
    let start_time = std::time::Instant::now();
    let mut incoming: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut out_degree = vec![0usize; n];
    for (source, path) in nodes.iter().enumerate() {
        for target in graph.get_neighborhood(path) {
            if let Some(&t) = index.get(target.as_str()) {
                incoming[t].push(source);
                out_degree[source] += 1;
            }
        }
    }

    tracing::debug!(
        seeds = seeds.len(),
        nodes = n,
        avg_incoming = incoming.iter().map(Vec::len).sum::<usize>() / n,
        "Starting Personalized PageRank"
    );

    let mut scores = vec![1.0 / n as f64; n];
    let mut next = vec![0.0; n];
    let mut converged_at = None;
    for iteration in 0..max_iter {
        for (t, sources) in incoming.iter().enumerate() {
            // Every source has out_degree >= 1 because it has an edge to t
            let sum: f64 = sources
                .iter()
                .map(|&s| scores[s] / out_degree[s] as f64)
                .sum();
            next[t] = damping * sum + (1.0 - damping) * personalization[t];
        }

        // Check convergence (L1 distance)
        let diff: f64 = scores.iter().zip(&next).map(|(a, b)| (a - b).abs()).sum();
        std::mem::swap(&mut scores, &mut next);

        if diff < tolerance {
            converged_at = Some(iteration);
            break;
        }
    }

    let elapsed = start_time.elapsed();
    if let Some(iter) = converged_at {
        tracing::debug!(
            seeds = seeds.len(),
            iterations = iter,
            elapsed_ms = elapsed.as_millis(),
            "PageRank converged"
        );
    } else {
        tracing::warn!(
            seeds = seeds.len(),
            max_iter = max_iter,
            elapsed_ms = elapsed.as_millis(),
            "PageRank did not converge"
        );
    }

    // Normalize so all scores are relative to the max score
    let max_score = scores.iter().copied().fold(0.0f64, f64::max);
    if max_score <= 0.0 {
        return HashMap::new();
    }

    nodes
        .into_iter()
        .zip(scores)
        .filter(|(_, score)| *score > 0.0)
        .map(|(path, score)| (path.clone(), score / max_score))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::path::PathBuf;

    fn links(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn seeds(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_linear_graph() {
        // A -> B -> C (bidirectional due to backlinks)
        let mut graph = GraphIndex::new();
        graph.update_note("A", PathBuf::from("A.md"), links(&["B"]));
        graph.update_note("B", PathBuf::from("B.md"), links(&["C"]));
        graph.update_note("C", PathBuf::from("C.md"), HashSet::new());

        let scores = personalized_pagerank(&graph, &seeds(&["A.md"]));

        let score_a = scores.get("A.md").copied().unwrap_or(0.0);
        let score_b = scores.get("B.md").copied().unwrap_or(0.0);
        let score_c = scores.get("C.md").copied().unwrap_or(0.0);

        // A is the seed, B is 1 hop away, C is 2 hops away
        assert!(score_a >= 0.7);
        assert!(score_b >= 0.5);
        assert!(score_c < score_b);
        assert!(score_c > 0.0);
    }

    #[test]
    fn test_disconnected_nodes() {
        // A -> B, C (disconnected)
        let mut graph = GraphIndex::new();
        graph.update_note("A", PathBuf::from("A.md"), links(&["B"]));
        graph.update_note("B", PathBuf::from("B.md"), HashSet::new());
        graph.update_note("C", PathBuf::from("C.md"), HashSet::new());

        let scores = personalized_pagerank(&graph, &seeds(&["A.md"]));

        let score_b = scores.get("B.md").copied().unwrap_or(0.0);
        let score_c = scores.get("C.md").copied().unwrap_or(0.0);
        assert!(score_b > score_c);
    }

    #[test]
    fn test_scores_are_keyed_by_subfolder_path() {
        let mut graph = GraphIndex::new();
        graph.update_note("Hub", PathBuf::from("projects/Hub.md"), links(&["Leaf"]));
        graph.update_note("Leaf", PathBuf::from("knowledge/Leaf.md"), HashSet::new());

        let scores = personalized_pagerank(&graph, &seeds(&["projects/Hub.md"]));

        assert!(scores.get("knowledge/Leaf.md").copied().unwrap_or(0.0) > 0.0);
        assert!(!scores.contains_key("Leaf"));
    }

    #[test]
    fn test_two_seeds_add_instead_of_multiplying() {
        // Two separate components: A - B and C - D
        let mut graph = GraphIndex::new();
        graph.update_note("A", PathBuf::from("A.md"), links(&["B"]));
        graph.update_note("B", PathBuf::from("B.md"), HashSet::new());
        graph.update_note("C", PathBuf::from("C.md"), links(&["D"]));
        graph.update_note("D", PathBuf::from("D.md"), HashSet::new());

        let scores = personalized_pagerank(&graph, &seeds(&["A.md", "C.md"]));

        // Each neighbor is close to one seed only; a product across seeds would be 0
        assert!(scores.get("B.md").copied().unwrap_or(0.0) > 0.0);
        assert!(scores.get("D.md").copied().unwrap_or(0.0) > 0.0);
    }

    #[test]
    fn test_same_name_notes_keep_separate_scores() {
        let mut graph = GraphIndex::new();
        graph.update_note("X", PathBuf::from("knowledge/X.md"), links(&["Z"]));
        graph.update_note("X", PathBuf::from("projects/X.md"), HashSet::new());
        graph.update_note("Z", PathBuf::from("Z.md"), HashSet::new());

        let scores = personalized_pagerank(&graph, &seeds(&["Z.md"]));

        let linked = scores.get("knowledge/X.md").copied().unwrap_or(0.0);
        let unlinked = scores.get("projects/X.md").copied().unwrap_or(0.0);
        assert!(linked > 0.0);
        assert!(linked > unlinked);
    }

    #[test]
    fn test_unknown_seed_returns_empty() {
        let mut graph = GraphIndex::new();
        graph.update_note("A", PathBuf::from("A.md"), HashSet::new());
        assert!(personalized_pagerank(&graph, &seeds(&["Missing.md"])).is_empty());
        assert!(personalized_pagerank(&graph, &[]).is_empty());
    }

    #[test]
    fn test_resolve_note_ref() {
        let mut graph = GraphIndex::new();
        graph.update_note("X", PathBuf::from("knowledge/X.md"), HashSet::new());
        graph.update_note("X", PathBuf::from("projects/X.md"), HashSet::new());
        graph.update_note("Plan", PathBuf::from("plans/Plan.md"), HashSet::new());

        assert_eq!(
            resolve_note_ref(&graph, "plans/Plan"),
            vec!["plans/Plan.md"]
        );
        assert_eq!(
            resolve_note_ref(&graph, "plans/Plan.md"),
            vec!["plans/Plan.md"]
        );
        assert_eq!(
            resolve_note_ref(&graph, "Plan#Section"),
            vec!["plans/Plan.md"]
        );
        assert_eq!(
            resolve_note_ref(&graph, "Plan|alias"),
            vec!["plans/Plan.md"]
        );
        assert_eq!(
            resolve_note_ref(&graph, "X"),
            vec!["knowledge/X.md", "projects/X.md"]
        );
        assert_eq!(
            resolve_note_ref(&graph, "projects/X"),
            vec!["projects/X.md"]
        );
        assert!(resolve_note_ref(&graph, "scratch/X").is_empty());
        assert!(resolve_note_ref(&graph, "Missing").is_empty());
        assert!(resolve_note_ref(&graph, "  ").is_empty());
    }
}
