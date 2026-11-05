# Obsidian Memory

Persistent memory for AI coding assistants.

Obsidian Memory is an [MCP](https://modelcontextprotocol.io/) server that lets Claude, OpenCode, and other AI assistants remember your projects, preferences, and past conversations by storing notes in your [Obsidian](https://obsidian.md) vault. Instead of starting fresh every session, your assistant can recall what you were working on, search through past decisions, and maintain context about your codebase.

**Who is this for?** Developers who use AI coding assistants and want them to actually remember things between sessions.

## Features

- **Graph navigation** - Wiki links, backlinks, neighborhood discovery
- **Semantic search** - Fast, offline embeddings (all-MiniLM-L6-v2) with Personalized PageRank graph boosting
- **Memory system** - Working Memory, Log, weekly journals, project notes
- **Project discovery** - Auto-loads project notes based on git remotes
- **Note management** - Create, read, update, move, and delete notes

## Installation

### Option 1: Homebrew (Mac)

```bash
brew tap webdesserts/tap
brew install webdesserts/tap/notes-sync
```

### Option 2: Shell Script (Mac/Linux)

```bash
curl -fsSL https://github.com/webdesserts/obsidian-memory/releases/latest/download/notes-sync-installer.sh | sh
```

### Option 3: Build from Source

Requires Rust toolchain ([rustup.rs](https://rustup.rs)).

```bash
git clone https://github.com/webdesserts/obsidian-memory.git
cd obsidian-memory
cargo install --path crates/notes-sync
```

Note: Building from source downloads the embedding model from HuggingFace at runtime on first launch. Pre-built binaries have the model embedded and work offline or on corporate networks that block HuggingFace.

### Option 4: Docker (Server Deployment)

For running as a persistent HTTP server behind a reverse proxy.

```bash
cd docker
DOMAIN=example.com NOTES_PATH=/path/to/vault docker compose up -d
```

| Variable | Required | Description |
|----------|----------|-------------|
| `DOMAIN` | Yes | Public domain for the service (used for TLS and WebAuthn origin, e.g., `example.com`) |
| `NOTES_PATH` | Yes | Absolute path to vault directory on the host |
| `VERSION` | No | Image tag (defaults to `latest`) |
| `RUST_LOG` | No | Log level (defaults to `info`) |

The compose file expects an external Docker network called `proxy` that your reverse proxy also joins. Create it before starting:

```bash
docker network create proxy
```

#### Reverse Proxy Requirements

The services don't handle TLS or authentication routing themselves — that's the reverse proxy's job. See [`docker/Caddyfile`](docker/Caddyfile) for a working Caddy example. Any reverse proxy needs to handle:

- **Auth endpoints** — `/auth/*` → `auth-service:3001` (strip the `/auth` prefix)
- **MCP endpoint** — `/mcp*` → `memory:3000` (protected via auth delegation to `auth-service:3001/validate`)
- **Sync WebSocket** — `/sync` → `memory:8080`

Important proxy considerations:
- The MCP endpoint uses SSE streaming — disable response buffering for `/mcp*`
- MCP operations (especially embedding computation) can be slow — set read/write timeouts to at least 5 minutes
- The auth-service `/validate` endpoint works with any proxy that supports forward-auth / auth delegation (Caddy `forward_auth`, nginx `auth_request`, Traefik `ForwardAuth`, etc.)

## Usage

### CLI Commands

```
notes mcp io --vault ~/notes                  # MCP stdio (launched by agent)
notes mcp up --vault ~/notes                  # MCP HTTP server (localhost:3000)
notes mcp up --vault ~/notes --listen 0.0.0.0:3000  # MCP HTTP on all interfaces
notes sync up --vault ~/notes                 # Sync daemon only
notes up --vault ~/notes                      # Both sync + MCP HTTP together
```

The `--vault` flag specifies the path to your Obsidian vault and is required on each subcommand.

### Running the Server

The server communicates over stdio and is designed to be launched by an MCP client. It indexes your vault on first run (may take a few seconds for large vaults) and watches for file changes.

### Claude Code Configuration

```bash
claude mcp add obsidian-memory --scope user \
  -- notes mcp io --vault ~/notes
```

### OpenCode Configuration

Add to `~/.config/opencode/opencode.json`:

```json
{
  "mcp": {
    "obsidian-memory": {
      "type": "local",
      "command": ["notes", "mcp", "io", "--vault", "~/notes"],
      "enabled": true
    }
  }
}
```

## How It Works

The server is read-only by default - it indexes your existing notes but won't modify them unless you explicitly use write tools. You can point it at an existing vault safely.

The memory system organizes notes into categories:

- **Temporary notes** (`Working Memory.md`, `Log.md`) - Scratchpad and activity log, frequently rewritten
- **Periodic notes** (`journal/`) - Weekly summaries that persist longer
- **Project notes** (`projects/`) - Context for specific codebases
- **Permanent notes** (`knowledge/`) - Stable reference material

### Memory Files

| File | Purpose | Auto-loaded |
|------|---------|-------------|
| `Working Memory.md` | Scratchpad for active work | Yes |
| `Log.md` | Chronological session activity | Yes |
| `journal/YYYY-wNN.md` | Weekly summaries and notes | Current week only |
| `projects/*.md` | Project-specific context | Matched by git remote URL or directory name |
| `knowledge/*.md` | Stable long-term notes | On demand |

### Search

The `Search` tool combines semantic embeddings with graph structure:

```json
{ "query": "typescript projects" }
{ "query": "[[TypeScript]]" }
{ "query": "[[TypeScript]] [[Projects]]" }
```

- Plain text queries use semantic similarity
- Wiki-links (like `[[TypeScript]]`) activate graph boosting - notes connected to the referenced note rank higher
- Multiple wiki-links find notes related to all referenced notes

## Available Tools

| Tool | Description |
|------|-------------|
| `Search` | Find notes by semantic similarity. Supports `query` and `debug` parameters |
| `ReadNote` | Read full content of a note, or just one section with the optional `section` param |
| `Outline` | Discover a note's addressable sections (frontmatter, preamble, headings) for section-scoped reads and writes on oversized notes |
| `WriteNote` | Create or overwrite a note. Supports the optional `section` param to scope to one section - creating it (and any missing ancestor headings) when content_hash is omitted, or overwriting it in place when content_hash is set |
| `EditNote` | Retired - merged into WriteNote. Calling it returns a teaching error pointing at WriteNote's `section` parameter |
| `ReplaceInNote` | Make text replacements in a note (find/replace) |
| `MoveNote` | Move/rename a note (automatically updates wiki-links in other notes) |
| `DeleteNote` | Delete a note from the vault |
| `GetNoteInfo` | Get metadata, frontmatter, and links for a note |
| `UpdateFrontmatter` | Update YAML frontmatter fields |

## Development

Requires Rust 1.85+ (edition 2024).

```bash
# Run tests
cargo test

# Run locally (downloads model from HuggingFace on first run)
cargo run -p notes-sync -- mcp io --vault ~/notes

# Build release
cargo build --release

# Build with embedded model (for testing release builds)
./scripts/download-model.sh
cargo build --features embedded-model -p notes-sync
```

## Troubleshooting

**Model download fails during build**: If you're behind a corporate firewall that blocks HuggingFace, use the pre-built binaries (Homebrew or shell installer) which have the model embedded.

**Vault not found**: Ensure the `--vault` path is an existing directory. Tilde expansion (`~/notes`) is supported.

**Search returns no results**: The semantic index builds on first run. Give it a few seconds to index your vault. Large vaults (1000+ notes) may take longer.

**Project notes not loading**: Project discovery looks for notes in `projects/` that match either your git remote URL (e.g., `github.com/user/repo`) or your current directory name. Create a note like `projects/my-repo.md` to get started.

## License

[Apache 2.0](LICENSE)
