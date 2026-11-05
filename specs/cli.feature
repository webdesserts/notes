Feature: CLI Commands
  The `notes` binary provides a unified CLI for managing MCP and sync
  services. There are two primary modes of operation, plus a future third:

  - **Client mode**: The MCP server runs on-demand via stdio, launched by
    the agent's MCP config. The Obsidian plugin handles sync independently.
    The user never interacts with the CLI directly — the agent does.

  - **Daemon mode**: The sync daemon and MCP HTTP server run together as
    long-lived services on a home server or VPS. The user starts them once
    and they stay running.

  - **Daemon-client mode** (future): The sync daemon runs continuously,
    but the MCP server is launched on-demand via stdio by the agent. The
    agent config either connects to a localhost HTTP server or invokes the
    CLI which manages a shared server behind the scenes.

  # --- Client Mode ---
  # The agent's MCP config invokes `notes mcp io --vault ~/notes` to start
  # an on-demand stdio session. No sync daemon needed — the Obsidian plugin
  # handles it.

  Scenario: Agent launches MCP server via stdio
    Given an agent MCP config that runs `notes mcp io --vault ~/notes`
    When the agent starts a session
    Then the MCP server should start with stdio transport
    And it should accept MCP protocol messages on stdin/stdout
    And it should exit when the agent disconnects

  Scenario: User runs `notes mcp` interactively
    When I run `notes mcp` in a terminal
    Then it should display help for MCP subcommands (io, up)
    And it should not start any server

  Scenario: Missing --vault shows error
    When I run `notes mcp io` without --vault
    Then it should display an error about the missing --vault flag
    And it should not start any server

  # --- Daemon Mode ---
  # On a home server or VPS, the user starts all services with one command.
  # Both sync and MCP HTTP run together, sharing the vault via filesystem.

  Scenario: Start all services on a home server
    Given a vault exists at "/home/user/notes"
    When I run `notes up --vault /home/user/notes`
    Then the sync daemon should start and listen for peer connections
    And the MCP HTTP server should start and listen for requests
    And both should run concurrently in the foreground

  Scenario: Start only the sync daemon
    Given a vault exists at "/home/user/notes"
    When I run `notes sync up --vault /home/user/notes`
    Then the sync daemon should start and listen for peer connections
    And the MCP server should not start

  Scenario: Start only the MCP HTTP server
    When I run `notes mcp up --vault ~/notes`
    Then the MCP server should start with HTTP transport
    And it should listen on 127.0.0.1:3000 by default
    And the sync daemon should not start

  Scenario: Graceful shutdown on Ctrl+C
    Given all services are running via `notes up`
    When I send SIGINT (Ctrl+C)
    Then both the sync daemon and MCP server should shut down gracefully

  # --- Configuration ---

  Scenario: Custom MCP HTTP listen address
    When I run `notes mcp up --vault ~/notes --listen 0.0.0.0:8080`
    Then the MCP server should listen on 0.0.0.0:8080

  Scenario: Sync daemon with advertised address
    Given a vault exists at "/home/user/notes"
    When I run `notes sync up --vault /home/user/notes --advertise ws://my.server.com:8080`
    Then peers should see the daemon at ws://my.server.com:8080

  # --- Help & Discoverability ---

  Scenario: No subcommand shows help
    When I run `notes` with no arguments
    Then it should display help text listing available subcommands
    And it should not start any server

  Scenario: Namespace shows subcommand help
    When I run `notes sync` with no further arguments
    Then it should display help for sync subcommands (up)
