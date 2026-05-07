---
slug: /
sidebar_position: 1
---

# Introduction

Trex is a single-binary, self-hosted backend platform built around an analytical
column-store engine. One container speaks Postgres wire, runs Deno edge
functions, hosts a plugin system for UIs and APIs, federates queries across
Postgres / MySQL / SQLite / BigQuery / ClickHouse / SAP HANA / S3 / HTTP, and
ships an identity provider, GraphQL API, REST API, MCP server, admin UI, and
Supabase-CLI-compatible management API on top.

## Who reads which section

| If you want to… | Start here |
|-----------------|------------|
| Run Trex on a single machine in 5 minutes | [Quickstart: Deploy](quickstarts/deploy) |
| Understand how Trex is built | [Concepts: Architecture](concepts/architecture) |
| Embed Trex in an existing app stack | [Tutorial: Embed Trex in your stack](tutorials/embed-trex-in-an-app) |
| Federate Postgres + S3 + HANA into one query | [Tutorial: Multi-Source Analytics](tutorials/multi-source-analytics) |
| Build a CDC-driven incremental warehouse | [Tutorial: Incremental Data Warehouse](tutorials/incremental-data-warehouse) |
| Use LLM inference inside SQL (RAG, vector search, summarisation) | [Tutorial: LLM-Augmented SQL](tutorials/llm-augmented-sql) |
| Drive Trex from an AI agent (Claude / Cursor) via MCP | [Tutorial: Agentic Trex with MCP](tutorials/agentic-trex-with-mcp) |
| Run a clinical-analytics workflow (OMOP / Atlas / FHIR / CQL) | [Tutorial: Clinical Analytics](tutorials/clinical-analytics) |
| Look up a SQL function | [SQL Reference](sql-reference/db) |
| Write a plugin | [Tutorial: Build a plugin](tutorials/build-a-plugin) → [Plugins](plugins/overview) |
| Publish a redistributable plugin | [Tutorial: Publish a Plugin](tutorials/publish-a-plugin) |
| Use the admin / management APIs from code | [APIs](apis/graphql) |
| Use the CLI | [CLI](cli) |
| Run Trex in production | [Deployment](deployment/docker) |

## Why a new project

Trex consolidates an analytical-first database, an application backend, and a
plugin layer into a single deployable unit. Most stacks force you to bolt these
together: a separate OLAP warehouse, a separate auth/API server, a separate
function runtime. Trex puts all three behind one Postgres-compatible
endpoint, so application data and analytical workloads share one engine and one
operational story.

This is opinionated and won't fit every shape — see
[Concepts → Architecture](concepts/architecture) for the tradeoffs.

## Getting started

If this is your first time, run the [5-minute deploy quickstart](quickstarts/deploy).
If you already know what Trex is and want a deep tour, jump to
[Concepts → Architecture](concepts/architecture).
