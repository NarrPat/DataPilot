"""Durable query acceptance, shared admission, outbox and ordered events.

No dispatcher/model worker is claimed by these tables. State writes are guarded
by the local service; runtime has no DDL privileges on app.
"""

from alembic import op

revision = "app_0002_tasks"
down_revision = "app_0001_auth"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE TABLE app.runtime_limits (\n        id integer PRIMARY KEY CHECK(id=1), "
        "active_reserved integer NOT NULL DEFAULT 0 CHECK(active_reserved BETWEEN 0 AND 2),\n  "
        "      logical_queued integer NOT NULL DEFAULT 0 CHECK(logical_queued BETWEEN 0 AND "
        "10))"
    )
    op.execute("INSERT INTO app.runtime_limits(id) VALUES(1)")
    op.execute(
        "CREATE TABLE app.analysis_threads (\n        thread_id uuid PRIMARY KEY, "
        "owner_user_id uuid NOT NULL REFERENCES app.users(user_id),\n        created_at "
        "timestamptz NOT NULL)"
    )
    op.execute(
        "CREATE TABLE app.query_runs (\n        query_id uuid PRIMARY KEY, thread_id uuid NOT "
        "NULL REFERENCES app.analysis_threads(thread_id),\n        owner_user_id uuid NOT NULL "
        "REFERENCES app.users(user_id), question text NOT NULL,\n        base_query_id uuid "
        "REFERENCES app.query_runs(query_id), base_spec_version integer,\n        "
        "effective_scope text NOT NULL CHECK(effective_scope IN ('all','east')), authz_version "
        "bigint NOT NULL,\n        status text NOT NULL CHECK(status IN ('queued','parsing',"
        "'generating','guarding','planning','executing','validating','repairing',"
        "'needs_clarification','completed','rejected','needs_refine','failed','timed_out',"
        "'canceled')),\n        status_version bigint NOT NULL DEFAULT 1 "
        "CHECK(status_version>=1), spec_version integer NOT NULL DEFAULT 0 "
        "CHECK(spec_version>=0),\n        execution_generation integer NOT NULL DEFAULT 1 "
        "CHECK(execution_generation>=1),\n        admission_state text NOT NULL "
        "CHECK(admission_state IN ('none','queued','active')),\n        cancel_requested "
        "boolean NOT NULL DEFAULT false, api_received_at timestamptz NOT NULL,\n        "
        "budget_ms integer NOT NULL DEFAULT 120000 CHECK(budget_ms=120000), waiting_intervals "
        "jsonb NOT NULL DEFAULT '[]',\n        catalog_version text NOT NULL, data_version "
        "text NOT NULL, graph_version text NOT NULL,\n        model_call_count integer NOT "
        "NULL DEFAULT 0 CHECK(model_call_count BETWEEN 0 AND 4),\n        repair_count integer "
        "NOT NULL DEFAULT 0 CHECK(repair_count BETWEEN 0 AND 1),\n        last_event_seq "
        "bigint NOT NULL DEFAULT 0, min_event_seq bigint NOT NULL DEFAULT 1,\n        "
        "reason_code text, finished_at timestamptz,\n        CHECK((base_query_id IS "
        "NULL)=(base_spec_version IS NULL)),\n        CHECK((status='queued' AND "
        "admission_state='queued') OR\n              (status IN ('parsing','generating',"
        "'guarding','planning','executing','validating','repairing') AND "
        "admission_state='active') OR\n              (status IN ('needs_clarification',"
        "'completed','rejected','needs_refine','failed','timed_out','canceled') AND "
        "admission_state='none'))\n    )"
    )
    op.execute(
        "CREATE INDEX ix_queries_owner ON app.query_runs(owner_user_id,api_received_at,query_id)"
    )
    op.execute(
        "CREATE TABLE app.request_keys (\n        actor_id uuid NOT NULL REFERENCES "
        "app.users(user_id), method text NOT NULL, resource text NOT NULL,\n        "
        "request_key varchar(128) NOT NULL, payload_hash char(64) NOT NULL,\n        query_id "
        "uuid NOT NULL REFERENCES app.query_runs(query_id),\n        PRIMARY KEY(actor_id,"
        "method,resource,request_key))"
    )
    op.execute(
        "CREATE TABLE app.query_outbox (\n        outbox_id uuid PRIMARY KEY, query_id uuid "
        "NOT NULL REFERENCES app.query_runs(query_id),\n        execution_generation integer "
        "NOT NULL, kind text NOT NULL CHECK(kind IN ('create','resume','recover_fragment')),\n "
        "       created_at timestamptz NOT NULL, published_at timestamptz, claimed_at "
        "timestamptz, canceled_at timestamptz,\n        UNIQUE(query_id,execution_generation,"
        "kind))"
    )
    op.execute(
        "CREATE TABLE app.query_events (\n        query_id uuid NOT NULL REFERENCES "
        "app.query_runs(query_id), seq bigint NOT NULL,\n        event_type text NOT NULL, "
        "status_version bigint NOT NULL, spec_version integer NOT NULL,\n        occurred_at "
        "timestamptz NOT NULL, payload jsonb NOT NULL,\n        PRIMARY KEY(query_id,seq))"
    )
    for table in (
        "runtime_limits",
        "analysis_threads",
        "query_runs",
        "request_keys",
        "query_outbox",
        "query_events",
    ):
        op.execute(f"REVOKE ALL ON app.{table} FROM PUBLIC, analyst_all, analyst_east")
        op.execute(f"GRANT SELECT,INSERT,UPDATE,DELETE ON app.{table} TO app_runtime")


def downgrade() -> None:
    for table in (
        "query_events",
        "query_outbox",
        "request_keys",
        "query_runs",
        "analysis_threads",
        "runtime_limits",
    ):
        op.execute(f"DROP TABLE app.{table}")
