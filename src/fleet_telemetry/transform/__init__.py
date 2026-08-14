"""Silver by hand: run `sql/silver/*.sql` in filename order, and nothing else.

Phase 3 step 1, and it is meant to be replaced by dbt in issue #12. The whole point is what
this package does NOT do, so the list is worth reading before the code:

  * it does not know what depends on what. Order is the filename, chosen by a human;
  * it does not test anything. A model that produces wrong numbers produces them silently;
  * it does not track state. Every run rebuilds every table from scratch;
  * it does not know which tables went stale when a script halfway down the list failed.

Those four absences are the argument for dbt, and none of them lands if you start with dbt.
The naive-first rule in docs/learn/README.md:22-26 is the whole reason this exists.

WHY PYTHON IS HERE AT ALL, GIVEN "TRANSFORM IN SQL".
CLAUDE.md:135-139 divides the labour: Extract/Load in Python, Transform in SQL. This module
holds no business logic -- it opens a connection, reads files off disk in sorted order, and
executes them. Every rule about the data lives in sql/silver/*.sql, where it is diffable. A
`psql -f` list in the README would be equally naive, and was the alternative considered; it
lost because psql is not on PATH on the Windows setup this repo targets, and because a list of
commands in prose cannot report which of them already ran.
"""
