# Analytics migration boundary

Revision `0001` implements the five business tables in `raw`, ten fixed views in
`analytics_views` and one nonbusiness seed-version record in `analytics_meta`.
The bootstrap step must create `analytics_owner` (NOLOGIN), `analyst_all`,
`analyst_east`, `app_runtime` and the isolated target database. The migration
connection is `dp_bootstrap`; it must be able to create/own objects, transfer
ownership to `analytics_owner` and manage the target database ACL. It must never
be the API, worker or analytics runtime connection.

Run from `F:\work_proj2` to render SQL without a database or secrets:

```powershell
.\.venv\Scripts\python.exe -m alembic -c apps/datapilot/alembic.ini upgrade head --sql
```

Rendering is a syntax/authoring check only. It does not prove that PostgreSQL
accepted the DDL, that bootstrap roles exist, that view ownership and grants work,
or that fixture constraints and numeric results are correct. The emitted
`current_database()` ACL block acts on whichever database receives that SQL;
apply only through the project bootstrap's validated target. This migration does
not decide CONNECT grants: bootstrap must grant analytics CONNECT only to its
appropriate initialization and analyst roles, and deny analyst CONNECT to app.

Online use is programmatic; the caller supplies a SQLAlchemy connection, without
writing a URL into `alembic.ini` or Alembic logging:

```python
from alembic import command
from alembic.config import Config

config = Config("apps/datapilot/alembic.ini")
with engine.begin() as connection:  # engine comes from validated bootstrap config
    config.attributes["connection"] = connection
    command.upgrade(config, "head")
```

Database CHECK constraints cover only registered enums and row-local numeric or
required-time conditions. Fixture validation additionally checks row sums,
cumulative successful refunds, state/time consistency and event ordering.
`products.status` is NOT NULL text because the specification defines no enum.
No triggers, reset API or data-version replacement workflow is introduced here.

View owners are the nonlogin owner, with default view-owner permission behavior
and `security_barrier=true`; analysts have SELECT only on their own five views.
Both scope dimension views retain only customers/products referenced by visible
orders; region filtering uses the order's sales-region snapshot.

Downgrade drops only this revision's views/tables/schemas, without CASCADE. An
unexpected dependency therefore blocks it. It never drops databases/roles and
does not restore removed PUBLIC privileges. The Alembic version table remains
managed by Alembic in `public`; analysts receive no public-schema privileges.
