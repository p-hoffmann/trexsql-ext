---
sidebar_position: 1
---

# Deploy in 5 minutes

This walkthrough takes you from zero to a running Trex stack with an admin
account, a working GraphQL endpoint, and a SQL session into the analytical
engine. It assumes you have **Docker** and **`psql`** (or any Postgres client)
installed.

## 1. Clone and start the stack

```bash
git clone https://github.com/p-hoffmann/trexsql
cd trexsql
docker compose up -d
```

The first run pulls three images (Trex, Postgres 16, PostgREST) and starts
them. After ~30 seconds the stack is healthy.

```bash
docker compose ps
```

You should see three services in `running (healthy)` state.

## 2. Create the first admin user

The first user to register is automatically promoted to `admin`. Open the
admin UI:

```
http://localhost:8001/trex/
```

Sign up with any email + password. After registration you'll land on the
admin dashboard. (If sign-up appears disabled, ensure `auth.selfRegistration`
is set in the admin UI's settings — it's enabled by default in fresh
deployments.)

:::tip
For non-interactive bootstrapping, set `ADMIN_EMAIL=you@example.com` in the
`trex` service's environment before `docker compose up`. Any user matching
that email is auto-promoted to admin on registration.
:::

## 3. Try the GraphQL endpoint

Enable GraphiQL by setting `ENABLE_GRAPHIQL=true` in the compose file and
restarting:

```bash
echo "      ENABLE_GRAPHIQL: 'true'" >> docker-compose.yml   # add under trex.environment
docker compose up -d trex
```

Then open:

```
http://localhost:8001/trex/graphiql
```

Try a simple query against the auto-generated schema:

```graphql
query {
  allUsers(first: 5) {
    nodes {
      id
      email
      role
      createdAt
    }
  }
}
```

You should see your admin user.

## 4. Connect to the analytical engine via psql

The pgwire endpoint is published on host port `5433`:

```bash
psql -h localhost -p 5433 -U trex -d main
```

The default password is empty. Once connected, try a federated query:

```sql
-- attach a remote Postgres (use any reachable PG)
ATTACH 'postgresql://postgres:mypass@localhost:65433/testdb' AS pg (TYPE postgres);

-- query across both Trex storage and the attached Postgres
SELECT * FROM pg.public.<some_table> LIMIT 10;
```

For a deeper federation example, see
[Quickstart: Federate a Postgres database](federate-postgres).

## 5. Issue an API key for code access

Back in the admin UI, navigate to **Settings → API Keys** and create a key.
Copy the `trex_…` value — you'll need it for MCP, the CLI, or any
server-to-server integration.

```bash
# example: hit the MCP server with the new key
curl -X POST http://localhost:8001/trex/mcp \
  -H "Authorization: Bearer trex_…" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'
```

## What just happened

The compose file started three containers: Postgres (auth + plugin metadata),
Trex (everything else, in one process), and PostgREST (auto-REST over the
Postgres metadata DB). Inside the Trex container, the Rust binary loaded every
SQL extension out of `EXTENSION_DIR`, then the `trexas` HTTP server brought up
the Deno-based core management application — which mounted GraphQL, auth, MCP,
the edge-function runtime, and the plugin loader. See
[Concepts → Architecture](../concepts/architecture) for the full picture.

## Next steps

- **Explore data**: [Quickstart: Federate a Postgres database](federate-postgres).
- **Build something**: [Tutorial: Build a plugin](../tutorials/build-a-plugin).
- **Use the CLI**: [Quickstart: Connect with the CLI](connect-with-cli).
- **Scale out**: [Quickstart: Run a distributed cluster](distributed-cluster).
- **Production deploy**: [Deployment → Docker Compose](../deployment/docker)
  and [Deployment → Environment](../deployment/environment).
