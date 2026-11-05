# Obsidian Memory

Persistent memory for AI coding assistants.

Obsidian Memory is an [MCP](https://modelcontextprotocol.io/) server that lets Claude, OpenCode, and other AI assistants remember your projects, preferences, and past conversations by storing notes in your [Obsidian](https://obsidian.md) vault. Instead of starting fresh every session, your assistant can recall what you were working on, search through past decisions, and maintain context about your codebase.

**Who is this for?** Developers who use AI coding assistants and want them to actually remember things between sessions.

## Features

- **Graph navigation** - Wiki links, backlinks, neighborhood discovery
- **Semantic search** - Fast, offline embeddings (all-MiniLM-L6-v2) with Personalized PageRank graph boosting
- **Note recommendations** - Graph- and embedding-based `recommend` tool for related notes
- **Note management** - Create, read, update, move, and delete notes

## Installation

### Option 1: Homebrew (Mac)

```bash
brew tap webdesserts/tap
brew install webdesserts/tap/notes-sync
```

### Option 2: Shell Script (Mac/Linux)

```bash
curl -fsSL https://github.com/webdesserts/notes/releases/latest/download/notes-sync-installer.sh | sh
```

### Option 3: Build from Source

Requires Rust toolchain ([rustup.rs](https://rustup.rs)).

```bash
git clone https://github.com/webdesserts/notes.git
cd notes
cargo install --path crates/notes-sync
```

Note: Building from source downloads the embedding model from HuggingFace at runtime on first launch. Pre-built binaries have the model embedded and work offline or on corporate networks that block HuggingFace.

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
claude mcp add notes --scope user \
  -- notes mcp io --vault ~/notes
```

### OpenCode Configuration

Add to `~/.config/opencode/opencode.json`:

```json
{
  "mcp": {
    "notes": {
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

## License

[Apache 2.0](LICENSE)
