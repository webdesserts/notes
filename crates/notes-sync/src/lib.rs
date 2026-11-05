use rmcp::{
    ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router,
    transport::stdio,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;

use rmcp::transport::streamable_http_server::{
    StreamableHttpService, session::local::LocalSessionManager,
};

mod config;
mod embeddings;
mod graph;
mod projects;
mod tools;
mod watcher;

// `sections`/`storage` moved to the `notes-core` crate (autonomy#69); these
// re-exports keep every existing `crate::sections::...`/`crate::storage::...`
// path in `tools/*.rs` resolving unchanged.
use notes_core::sections;
use notes_core::storage;

pub use config::Config;
use embeddings::EmbeddingManager;
use graph::GraphIndex;
use storage::FileStorage;
use watcher::VaultWatcher;

/// Parameters for the Log tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LogParams {
    /// Timeline entry content (single bullet point). Tool adds timestamp and day headers automatically.
    /// Tag work items with associated jira tickets or github issues when relevant.
    pub content: String,
}

/// Parameters for the GetNoteInfo tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetNoteInfoParams {
    /// Note reference - supports: "memory:Note Name", "memory:knowledge/Note Name", "knowledge/Note Name", "[[Note Name]]"
    pub note: String,
}

/// Parameters for the UpdateFrontmatter tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateFrontmatterParams {
    /// Note reference - supports wiki-links ([[Note]]), memory URIs (memory:knowledge/Note), or plain names
    pub note: String,
    /// Frontmatter fields to update
    pub updates: std::collections::HashMap<String, serde_json::Value>,
    /// Content hash from ReadNote - required to verify note hasn't changed
    pub content_hash: String,
}

/// Parameters for the Search tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchParams {
    /// The search query - what information are you looking for? Supports wiki-links: [[Note]] searches using that note's content. Multiple notes: [[TypeScript]] [[Projects]] finds notes similar to BOTH. Mixed: 'type safety in [[TypeScript]]' combines note content with text. Wiki-links enable graph boosting (connected notes rank higher).
    pub query: String,
    /// Show detailed score breakdown (semantic, graph proximity, boost calculation). Useful for understanding how results are ranked.
    #[serde(default)]
    pub debug: bool,
}

/// Parameters for the Recommend tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecommendParams {
    /// Free text describing the current focus. [[wiki-links]] in it are used as seeds too.
    #[serde(default)]
    pub query: String,
    /// Seed notes for graph proximity: relative paths (projects/X or projects/X.md) or note names. Seeds are never returned.
    #[serde(default)]
    pub seeds: Vec<String>,
    /// Notes to leave out of the results: relative paths or note names (e.g. notes already open).
    #[serde(default)]
    pub exclude: Vec<String>,
    /// Maximum number of results (default 5, max 20).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Parameters for the WriteLogs tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriteLogsParams {
    /// ISO week date in YYYY-Www-D format (e.g., '2025-W50-1' for Monday of week 50). Week starts on Monday (1=Mon, 7=Sun).
    #[serde(rename = "isoWeekDate")]
    pub iso_week_date: String,
    /// Object mapping time strings to log messages. Keys: '9:00 AM', '2:30 PM', etc. (12-hour format with AM/PM). Values: Log entry content. Example: { '9:00 AM': 'Started investigation', '2:30 PM': 'Fixed bug #123' }
    pub entries: std::collections::HashMap<String, String>,
}

/// Parameters for the Remember tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RememberParams {
    /// The client's current working directory path. Used for project discovery
    /// via git remote and directory name matching. When omitted (e.g. on iOS
    /// or other clients without a filesystem), project discovery is skipped
    /// but Log, Working Memory, and the weekly note are still loaded.
    pub cwd: Option<String>,
}

/// Parameters for the ReadNote tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadNoteParams {
    /// Note reference - supports wiki-links ([[Note]]), memory URIs (memory:knowledge/Note), or plain names
    pub note: String,
    /// Optional section path (from the outline tool's `path` field) to read just
    /// one section instead of the whole note, e.g. "Daily Log > 2026-W26-4".
    /// content_hash then scopes to that section only.
    #[serde(default)]
    pub section: Option<String>,
}

/// Parameters for the Outline tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutlineParams {
    /// Note reference - supports wiki-links ([[Note]]), memory URIs (memory:knowledge/Note), or plain names
    pub note: String,
}

/// Parameters for the WriteNote tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriteNoteParams {
    /// Note reference - supports wiki-links ([[Note]]), memory URIs (memory:knowledge/Note), or plain names
    pub note: String,
    /// The content to write. For a whole-note write, the entire note
    /// content. When `section` is set: a create (content_hash omitted)
    /// takes body-only content and the heading line is synthesized
    /// automatically; an edit (content_hash set) takes the section's full
    /// replacement text, including its own heading line.
    pub content: String,
    /// Content hash from ReadNote/Outline. Omit to create a new note or a
    /// new section; required to overwrite an existing note or edit an
    /// existing section.
    pub content_hash: Option<String>,
    /// Optional section path (from the Outline tool's `path` field) to
    /// scope the write to one section instead of the whole note - creating
    /// it (and any missing ancestor headings) when content_hash is omitted,
    /// or overwriting it in place when content_hash is set.
    #[serde(default)]
    pub section: Option<String>,
}

/// A single find-and-replace operation for the ReplaceInNote tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReplaceOperation {
    /// Text to search for - must match exactly and appear only once
    #[serde(rename = "oldText")]
    pub old_text: String,
    /// Text to replace with
    #[serde(rename = "newText")]
    pub new_text: String,
}

/// Parameters for the ReplaceInNote tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReplaceInNoteParams {
    /// Note reference - supports wiki-links ([[Note]]), memory URIs (memory:knowledge/Note), or plain names
    pub note: String,
    /// Array of edit operations. Each oldText must appear exactly once within
    /// the note.
    pub edits: Vec<ReplaceOperation>,
    /// Content hash from ReadNote - required to verify note hasn't changed.
    pub content_hash: String,
    /// Preview changes without applying them (default: false)
    #[serde(default, rename = "dryRun")]
    pub dry_run: bool,
    /// Not supported by replace_in_note, which always replaces within the
    /// whole note. Present only so a section-scoped request can be rejected
    /// with a clear, teaching error instead of a generic "unknown field"
    /// message. Use write_note with its own `section` parameter for
    /// section-scoped writes.
    #[serde(default)]
    pub section: Option<String>,
}

/// Reject replace_in_note calls that pass `section` — replace_in_note always
/// matches within the whole note, so a section-scoped request is a client
/// mistake. Returns a teaching error naming the tool that does support
/// section-scoped writes.
fn reject_replace_in_note_section(section: Option<&str>) -> Result<(), ErrorData> {
    if section.is_some() {
        return Err(ErrorData::invalid_params(
            "replace_in_note does not support section-scoped writes. Use \
             write_note with its `section` parameter instead.",
            None,
        ));
    }
    Ok(())
}

/// Parameters for the DeleteNote tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeleteNoteParams {
    /// Note reference - supports wiki-links ([[Note]]), memory URIs (memory:knowledge/Note), or plain names
    pub note: String,
}

/// Parameters for the Backlinks tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BacklinksParams {
    /// Note reference - supports wiki-links ([[Note]]), memory URIs (memory:knowledge/Note), or plain names
    pub note: String,
}

/// Parameters for the MoveNote tool
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MoveNoteParams {
    /// Source note reference
    pub from: String,
    /// Destination note reference
    pub to: String,
}

/// Shared state that can be reused across multiple HTTP sessions.
/// Pre-initialized once, then passed to each session's MemoryServer.
#[derive(Clone)]
pub struct SharedState {
    config: Arc<Config>,
    graph: Arc<RwLock<GraphIndex>>,
    embeddings: Arc<EmbeddingManager>,
    storage: Arc<FileStorage>,
    /// File watcher handle - kept alive for the lifetime of the shared state.
    #[allow(dead_code)]
    watcher: Option<Arc<VaultWatcher>>,
}

impl SharedState {
    /// Initialize shared state (async, call once before starting HTTP server).
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        // Initialize graph index by scanning the vault
        let mut graph = GraphIndex::new();
        graph.initialize(&config.vault_path).await?;

        let graph = Arc::new(RwLock::new(graph));

        // Create embedding manager and preload model + embeddings at startup
        let embeddings = Arc::new(EmbeddingManager::new(&config.vault_path));

        // Spawn background task to preload embeddings
        // Server starts immediately - search will wait for model but not for preload
        {
            let graph_clone = graph.clone();
            let embeddings_clone = embeddings.clone();
            let vault_path = config.vault_path.clone();

            tokio::spawn(async move {
                // Collect paths first, then drop lock before doing I/O
                let paths: Vec<String> = {
                    let graph_read = graph_clone.read().await;
                    graph_read.all_paths().cloned().collect()
                };

                // Read files asynchronously without holding lock
                let mut notes = Vec::with_capacity(paths.len());
                for path in paths {
                    let full_path = vault_path.join(&path);
                    if let Ok(content) = tokio::fs::read_to_string(&full_path).await {
                        notes.push((path, content));
                    }
                }

                if !notes.is_empty() {
                    tracing::info!(
                        "Preloading embeddings for {} notes in background...",
                        notes.len()
                    );
                    if let Err(e) = embeddings_clone.get_embeddings_batch(&notes).await {
                        tracing::warn!(
                            "Failed to preload embeddings: {}. First search will be slower.",
                            e
                        );
                    } else {
                        tracing::info!("Embeddings preloaded successfully");
                    }
                }
            });
        }

        // Create storage backend
        let storage = Arc::new(FileStorage::new(config.vault_path.clone()));

        // Start file watcher to keep graph index and embeddings up to date
        let watcher =
            match VaultWatcher::start(config.vault_path.clone(), graph.clone(), embeddings.clone())
            {
                Ok(w) => {
                    tracing::info!("File watcher started successfully");
                    Some(Arc::new(w))
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to start file watcher: {}. Graph index will not auto-update.",
                        e
                    );
                    None
                }
            };

        Ok(Self {
            config: Arc::new(config),
            graph,
            embeddings,
            storage,
            watcher,
        })
    }
}

/// The main MCP server state, holding configuration and shared resources.
#[derive(Clone)]
pub struct MemoryServer {
    /// Shared state (graph, embeddings, storage, config) - same across all sessions
    shared: SharedState,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl MemoryServer {
    /// Create a new server for stdio transport (single client).
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        let shared = SharedState::new(config).await?;
        Ok(Self::from_shared(shared))
    }

    /// Create a server from pre-initialized shared state (sync, for HTTP factory).
    pub fn from_shared(shared: SharedState) -> Self {
        Self {
            shared,
            tool_router: Self::tool_router(),
        }
    }

    // Accessor methods for shared state fields
    fn config(&self) -> &Config {
        &self.shared.config
    }

    fn graph(&self) -> &Arc<RwLock<GraphIndex>> {
        &self.shared.graph
    }

    fn embeddings(&self) -> &Arc<EmbeddingManager> {
        &self.shared.embeddings
    }

    fn storage(&self) -> &FileStorage {
        &self.shared.storage
    }

    #[tool(
        description = "Get the current date and time in ISO format for use in Working Memory timeline entries. Returns ISO 8601 formatted datetime (YYYY-MM-DDTHH:MM) and additional context."
    )]
    async fn get_current_datetime(&self) -> Result<CallToolResult, ErrorData> {
        tools::get_current_datetime::execute()
    }

    #[tool(
        description = "Append a timestamped entry to Log.md for active work state and debugging context tracking. Records chronological session activity - what happened when. The tool automatically adds timestamps and organizes entries by day. Use this for tracking work in progress, debugging steps, state changes, and decisions made during active work."
    )]
    async fn log(&self, params: Parameters<LogParams>) -> Result<CallToolResult, ErrorData> {
        tools::log::execute(&self.config().vault_path, &params.0.content).await
    }

    #[tool(
        description = "Get metadata and graph connections for the current week's journal note. Returns path, URIs, frontmatter, and links/backlinks. Works whether or not the note exists yet. Use ReadNote tool to get content."
    )]
    async fn get_weekly_note_info(&self) -> Result<CallToolResult, ErrorData> {
        let graph = self.graph().read().await;
        tools::get_weekly_note_info::execute(
            &self.config().vault_path,
            &self.config().vault_name,
            &graph,
        )
        .await
    }

    #[tool(
        description = "Get metadata and graph connections for a note. Returns frontmatter, file paths, and links/backlinks. Use ReadNote tool to get content."
    )]
    async fn get_note_info(
        &self,
        params: Parameters<GetNoteInfoParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let graph = self.graph().read().await;
        tools::get_note_info::execute(
            &self.config().vault_path,
            &self.config().vault_name,
            &graph,
            &params.0.note,
        )
        .await
    }

    #[tool(
        description = "Update frontmatter metadata in a note. Requires content_hash from ReadNote. Returns JSON with new content_hash."
    )]
    async fn update_frontmatter(
        &self,
        params: Parameters<UpdateFrontmatterParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let graph = self.graph().read().await;
        tools::update_frontmatter::execute(
            self.storage(),
            &graph,
            &params.0.note,
            params.0.updates,
            &params.0.content_hash,
        )
        .await
    }

    #[tool(
        description = "Load all session context files in a single call. Returns Log.md, Working Memory.md, current weekly note, and discovered project notes. Automatically discovers projects based on git remotes and directory names. Use this at the start of every session to get complete context about recent work, current focus, this week's activity, and project context."
    )]
    async fn remember(
        &self,
        params: Parameters<RememberParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let graph = self.graph().read().await;
        let cwd = params.0.cwd.map(std::path::PathBuf::from);
        tools::remember::execute(&self.config().vault_path, &graph, cwd.as_deref()).await
    }

    #[tool(
        description = "Search for relevant notes using semantic similarity. Encodes the query and compares it against all note embeddings. Returns similarity-ordered list of potentially relevant notes. Supports note references via wiki-links: [[Note Name]]"
    )]
    async fn search(&self, params: Parameters<SearchParams>) -> Result<CallToolResult, ErrorData> {
        let graph = self.graph().read().await;
        tools::search::execute(
            &self.config().vault_path,
            &graph,
            self.embeddings(),
            &params.0.query,
            params.0.debug,
        )
        .await
    }

    #[tool(
        description = "Recommend related notes for an agent's current focus. Combines semantic similarity to the query and seed notes with Personalized PageRank proximity to the seeds (score = semantic + 0.3 × graph). Returns JSON structured content {query, seeds, results: [{path, name, score, semantic, graph}]}; seeds and excluded notes are never returned."
    )]
    async fn recommend(
        &self,
        params: Parameters<RecommendParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let graph = self.graph().read().await;
        tools::search::recommend(
            &self.config().vault_path,
            &graph,
            self.embeddings(),
            &params.0.query,
            &params.0.seeds,
            &params.0.exclude,
            params.0.limit,
        )
        .await
    }

    #[tool(
        description = "Replace an entire day's log entries with consolidated/compacted entries. Use this ONLY during memory consolidation to rewrite or summarize a day's logs. For adding new entries during active work, use the Log tool instead (it's simpler and doesn't require reading the log first). This tool automatically formats entries with correct timestamps, en-dashes, and chronological sorting. Pass an empty object to delete the entire day section (header and all entries)."
    )]
    async fn write_logs(
        &self,
        params: Parameters<WriteLogsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        tools::write_logs::execute(
            &self.config().vault_path,
            &params.0.iso_week_date,
            params.0.entries,
        )
        .await
    }

    #[tool(
        description = "Review active context (Log.md, Working Memory.md, current weekly journal, project notes) and consolidate content into permanent storage. Optimizes token usage by keeping active/relevant work accessible while compressing or archiving finished work. Applies information lifecycle: active work = keep lean, shipped/merged = compress and archive. Returns detailed consolidation instructions."
    )]
    async fn reflect(&self) -> Result<CallToolResult, ErrorData> {
        tools::reflect::execute()
    }

    #[tool(
        description = "Read the complete contents of a note. Returns JSON with content and content_hash. Content includes line numbers (cat -n format: right-aligned number + tab). content_hash is computed on raw content — pass it through to WriteNote or ReplaceInNote unchanged."
    )]
    async fn read_note(
        &self,
        params: Parameters<ReadNoteParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let graph = self.graph().read().await;
        tools::read_note::execute_scoped(
            self.storage(),
            &graph,
            &params.0.note,
            params.0.section.as_deref(),
        )
        .await
    }

    #[tool(
        description = "Discover a note's addressable sections (frontmatter, preamble, and heading-delimited sections) for section-scoped reads and writes on oversized notes. This is a fallback for oversized notes, not the primary editing path — prefer small, heavily-linked notes where possible. Returns a flat list of sections, each with a `path` — the literal string to pass as the `section` param of ReadNote or WriteNote."
    )]
    async fn outline(
        &self,
        params: Parameters<OutlineParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let graph = self.graph().read().await;
        tools::outline::execute(self.storage(), &graph, &params.0.note).await
    }

    #[tool(
        description = "Create a new note or overwrite an existing note. For existing notes, include content_hash from ReadNote. Returns JSON with new content_hash for chained writes. Optionally pass `section` (a path from the Outline tool) to scope the write to one section: omit content_hash to create it (and any missing ancestor headings, synthesized automatically) with body-only content, or set content_hash to overwrite an existing section in place with its full replacement text including the heading line."
    )]
    async fn write_note(
        &self,
        params: Parameters<WriteNoteParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let graph = self.graph().read().await;
        tools::write_note::execute_scoped(
            &self.config().vault_path,
            self.storage(),
            &graph,
            &params.0.note,
            &params.0.content,
            params.0.content_hash.as_deref(),
            params.0.section.as_deref(),
        )
        .await
    }

    #[tool(
        description = "Make surgical text replacements in a note. Each edit specifies oldText (must match exactly and appear once) and newText. Requires content_hash from ReadNote. Returns JSON with new content_hash for chained edits."
    )]
    async fn replace_in_note(
        &self,
        params: Parameters<ReplaceInNoteParams>,
    ) -> Result<CallToolResult, ErrorData> {
        reject_replace_in_note_section(params.0.section.as_deref())?;

        let edits: Vec<tools::replace_in_note::Edit> = params
            .0
            .edits
            .into_iter()
            .map(|e| tools::replace_in_note::Edit {
                old_text: e.old_text,
                new_text: e.new_text,
            })
            .collect();

        let graph = self.graph().read().await;
        tools::replace_in_note::execute(
            &self.config().vault_path,
            self.storage(),
            &graph,
            &params.0.note,
            edits,
            &params.0.content_hash,
            params.0.dry_run,
        )
        .await
    }

    #[tool(
        description = "Permanently delete a note from the vault. Returns an error if the note doesn't exist."
    )]
    async fn delete_note(
        &self,
        params: Parameters<DeleteNoteParams>,
    ) -> Result<CallToolResult, ErrorData> {
        tools::delete_note::execute(
            &self.config().vault_path,
            self.storage(),
            self.graph(),
            &params.0.note,
        )
        .await
    }

    #[tool(
        description = "List a note's backlinks (notes that link to it) and its outgoing wiki-links, using the vault's link graph."
    )]
    async fn backlinks(
        &self,
        params: Parameters<BacklinksParams>,
    ) -> Result<CallToolResult, ErrorData> {
        tools::backlinks::execute(self.storage(), self.graph(), &params.0.note).await
    }

    #[tool(
        description = "Move or rename a note. Automatically updates wiki-links in all notes that reference the moved note. Fails if destination already exists."
    )]
    async fn move_note(
        &self,
        params: Parameters<MoveNoteParams>,
    ) -> Result<CallToolResult, ErrorData> {
        tools::move_note::execute(
            &self.config().vault_path,
            self.storage(),
            self.graph(),
            &params.0.from,
            &params.0.to,
        )
        .await
    }
}

#[tool_handler]
impl rmcp::ServerHandler for MemoryServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::LATEST,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "obsidian-memory".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                ..Default::default()
            },
            instructions: Some(
                "Obsidian Memory MCP server - provides tools for managing notes and memory in an Obsidian vault."
                    .into(),
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Server functions
// ---------------------------------------------------------------------------

/// Run the server with STDIO transport.
pub async fn run_stdio_server(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Vault path: {}", config.vault_path.display());
    let server = MemoryServer::new(config).await?;

    let service = server.serve(stdio()).await.inspect_err(|e| {
        tracing::error!("Error starting server: {}", e);
    })?;

    tracing::info!("Obsidian Memory MCP server started (stdio)");
    service.waiting().await?;

    Ok(())
}

/// Health check router, shared between the server and tests.
pub fn health_router() -> axum::Router {
    axum::Router::new().route("/health", axum::routing::get(|| async { "OK" }))
}

/// Run the server with HTTP transport, shutting down on SIGINT/SIGTERM.
///
/// Used by the `notes` CLI. Embedders that own their own lifecycle (the
/// NotesSync desktop app) should call [`serve_http`] with their own shutdown
/// future instead, so no process-wide signal handler is installed.
pub async fn run_http_server(
    config: Config,
    listen: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    serve_http(config, listen, memory_common::shutdown_signal()).await
}

/// Run the MCP HTTP server (`/mcp` + `/health`) until `shutdown` resolves.
pub async fn serve_http(
    config: Config,
    listen: &str,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Vault path: {}", config.vault_path.display());

    let shared = Arc::new(SharedState::new(config).await?);

    let service = StreamableHttpService::new(
        {
            let shared = shared.clone();
            move || Ok(MemoryServer::from_shared((*shared).clone()))
        },
        LocalSessionManager::default().into(),
        Default::default(),
    );

    let router = axum::Router::new()
        .merge(health_router())
        .nest_service("/mcp", service);

    let addr: std::net::SocketAddr = listen
        .parse()
        .map_err(|e| format!("Invalid listen address '{}': {}", listen, e))?;

    if addr.ip().is_unspecified() {
        tracing::info!(
            "Binding to all interfaces ({}). Ensure a reverse proxy handles authentication.",
            addr.ip()
        );
    }

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("Failed to bind to {} - {}", listen, e))?;

    tracing::info!(
        "Obsidian Memory MCP server started (HTTP) at http://{}/mcp",
        addr
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    /// Build a real `MemoryServer` over a fresh vault path, for wiring
    /// defeat-checks that need to prove a guard is actually called from
    /// inside the real `#[tool]`-generated handler - not just callable in
    /// isolation as a free function. `SharedState` is built as a raw struct
    /// literal (this module can see its private fields, since `mod tests`
    /// is a descendant of the module that defines them): a bare, unpopulated
    /// `GraphIndex::new()` (note resolution falls back to `storage.exists`
    /// for top-level note names with no subdirectory - see
    /// `tools::common::resolve_note_uri` - so fixtures just need a file on
    /// disk, not graph registration, for the tests that use this helper
    /// today), `EmbeddingManager::new` (confirmed synchronous/cheap - it
    /// only sets up paths, no model load until `.initialize()`/first
    /// search), `FileStorage::new`, and no watcher.
    fn test_server(vault_path: &std::path::Path) -> super::MemoryServer {
        let shared = super::SharedState {
            config: std::sync::Arc::new(super::Config::new(&vault_path.to_string_lossy())),
            graph: std::sync::Arc::new(tokio::sync::RwLock::new(super::GraphIndex::new())),
            embeddings: std::sync::Arc::new(super::EmbeddingManager::new(vault_path)),
            storage: std::sync::Arc::new(super::FileStorage::new(vault_path.to_path_buf())),
            watcher: None,
        };
        super::MemoryServer::from_shared(shared)
    }

    // D7 wiring defeat-checks (memory/o:9): each of these calls the real
    // `#[tool]`-generated async method through `test_server`, proving the
    // named guard/stub is wired into the actual handler's call path -
    // exercising it in isolation (as a free function) can't prove that.

    #[tokio::test]
    async fn test_reject_replace_in_note_section_wired_into_real_handler() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let original_content = "Original content, must survive a rejected replace_in_note call.";
        tokio::fs::write(temp_dir.path().join("test.md"), original_content)
            .await
            .unwrap();

        let server = test_server(temp_dir.path());
        let result = server
            .replace_in_note(rmcp::handler::server::wrapper::Parameters(
                super::ReplaceInNoteParams {
                    note: "test".to_string(),
                    edits: vec![super::ReplaceOperation {
                        old_text: "Original".to_string(),
                        new_text: "REPLACED - must never land on disk".to_string(),
                    }],
                    content_hash: "irrelevant-to-this-test".to_string(),
                    dry_run: false,
                    section: Some("Some Section".to_string()),
                },
            ))
            .await;

        let err = result.expect_err("replace_in_note must reject a section-scoped request");
        assert!(err.message.contains("write_note"));

        let on_disk = tokio::fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(on_disk, original_content);
    }

    #[tokio::test]
    async fn test_write_note_section_create_through_real_handler() {
        let temp_dir = tempfile::TempDir::new().unwrap();

        let server = test_server(temp_dir.path());
        let result = server
            .write_note(rmcp::handler::server::wrapper::Parameters(
                super::WriteNoteParams {
                    note: "test".to_string(),
                    content: "first entry".to_string(),
                    content_hash: None,
                    section: Some("Daily Log".to_string()),
                },
            ))
            .await
            .expect("section create through the real handler should succeed");
        assert!(!result.is_error.unwrap_or(false));

        let on_disk = tokio::fs::read_to_string(temp_dir.path().join("test.md"))
            .await
            .unwrap();
        assert_eq!(on_disk, "# Daily Log\nfirst entry");
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let router = super::health_router();

        let request = axum::http::Request::builder()
            .uri("/health")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"OK");
    }

    // Regression coverage for memory/o:8: a tool params struct that silently
    // tolerates unknown fields lets a client-side typo (or an invented
    // parameter, like the incident's `section` on write_note) pass through
    // deserialization unnoticed instead of surfacing as an error.

    #[test]
    fn test_deny_unknown_fields_rejects_unknown_field_on_update_frontmatter() {
        let payload = serde_json::json!({
            "note": "test",
            "updates": {},
            "content_hash": "abc123",
            "bogus_field": "nope",
        });

        let result: Result<super::UpdateFrontmatterParams, _> = serde_json::from_value(payload);
        let err = result.expect_err("unknown field should be rejected, not silently ignored");
        assert!(
            err.to_string().contains("bogus_field"),
            "error should name the offending field, got: {}",
            err
        );
    }

    #[test]
    fn test_deny_unknown_fields_rejects_unknown_field_on_write_note() {
        let payload = serde_json::json!({
            "note": "test",
            "content": "hello",
            "bogus_field": "nope",
        });

        let result: Result<super::WriteNoteParams, _> = serde_json::from_value(payload);
        let err = result.expect_err("unknown field should be rejected, not silently ignored");
        assert!(
            err.to_string().contains("bogus_field"),
            "error should name the offending field, got: {}",
            err
        );
    }
}
