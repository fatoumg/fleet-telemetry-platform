"""Execute sql/silver/*.sql in filename order. Deliberately as dumb as that sounds.

    python -m fleet_telemetry.transform.run
    python -m fleet_telemetry.transform.run --sql-dir some/other/dir

THE THINGS THIS DOES NOT DO ARE THE POINT (see the package docstring). Two of them are worth
spelling out here, because they shape the output:

**Order is lexical, and nothing checks it.** `40_ping_quality.sql` reads a table that
`10_stg_pings.sql` builds. Nothing in this file knows that. Rename one and the other fails --
or worse, succeeds against yesterday's copy of the table, which is the silent case.

**Each script owns a transaction, and a failure leaves a MIX.** Scripts before the failure are
committed; scripts after it never run; the failing script's own table rolls back to its
*previous* contents rather than vanishing. So after a failed run some tables are fresh, some
are stale, one is stale-but-looks-fine, and nothing anywhere records which is which. Reporting
that state is this module's most useful output, which is why the failure path prints three
lists instead of just an error.

The alternative -- one transaction around every script -- was rejected: it would make the whole
layer atomic and hide exactly the failure this phase exists to demonstrate. A tool that makes
the naive version look better than it is teaches nothing.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from psycopg import Connection, connect
from psycopg import Error as PostgresError

from fleet_telemetry import config

# Hand-written Silver lands in its own schema, NOT in `silver`.
#
# dbt owns `silver`: dbt/dbt_project.yml:19-23 materialises every staging model there as a
# VIEW. Hand-built tables with the same names in the same schema would be dropped by dbt the
# first time issue #12 runs, which destroys the artifact this phase produces. Keeping them
# apart means both survive, and the diff between them becomes free:
#
#   select * from silver_manual.stg_pings except select * from silver.stg_pings;
#
# Where those disagree, one of the two is wrong -- the same shape of deliverable as phase 2's
# poller-vs-CDC comparison. The model names are deliberately identical to make that one-liner
# work; only the schema differs.
SCHEMA = "silver_manual"

# Repo root, four levels up from src/fleet_telemetry/transform/run.py. Correct under
# `pip install -e .` (the only install this project documents) because an editable install
# leaves the package in the working tree. --sql-dir exists so tests need not rely on it.
SQL_DIR = Path(__file__).resolve().parents[3] / "sql" / "silver"


def discover(directory: Path) -> list[Path]:
    """Every .sql file, sorted by filename. That sort IS the dependency graph.

    Numeric prefixes (00_, 10_, 20_) exist so the sort is stable and readable, and so inserting
    a model between two others is a visible renaming decision rather than an accident. Note
    what a human has to do here that dbt does automatically: know that 40 reads what 10 writes,
    and never let the numbers stop reflecting that.
    """
    return sorted(directory.glob("*.sql"))


def apply_schema(conn: Connection) -> None:
    """Create the target schema if it is missing.

    Here rather than in docker/warehouse/init.sql, deliberately. That file runs only when the
    data directory is empty, so DDL added to it never reaches a volume that already exists --
    which is exactly how the four REPLICA IDENTITY FULL statements sat unexecuted for five days
    (docs/known-issues.md, section 1). A schema created by the thing that needs it cannot
    develop that gap.
    """
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    conn.commit()


def run_script(conn: Connection, path: Path) -> float:
    """Execute one file as one transaction. Returns elapsed seconds; raises on failure.

    The whole file goes to one execute() call, so a script is free to hold several statements
    (every model here is a DROP followed by a CREATE TABLE AS). No per-statement row count is
    reported: it would be the count from whichever statement happened to be last, which is a
    number that looks meaningful and is not. Row counts come from summarise() instead, by
    counting the tables that actually exist.
    """
    started = time.monotonic()
    try:
        with conn.cursor() as cur:
            cur.execute(path.read_text(encoding="utf-8"))
        conn.commit()
    except PostgresError:
        conn.rollback()
        raise
    return time.monotonic() - started


def summarise(conn: Connection) -> list[tuple[str, int]]:
    """(table, rows) for everything in the schema, counted rather than estimated.

    reltuples from pg_class would be cheaper and is a planner estimate, which is not good
    enough for a layer whose entire claim is that its numbers can be checked.
    """
    with conn.cursor() as cur:
        cur.execute(
            "select table_name from information_schema.tables "
            "where table_schema = %s order by table_name",
            (SCHEMA,),
        )
        tables = [row[0] for row in cur.fetchall()]

        counts = []
        for table in tables:
            # Identifier interpolation, not a parameter -- table names cannot be bound. The
            # value comes from information_schema, never from user input.
            cur.execute(f"select count(*) from {SCHEMA}.{table}")
            counts.append((table, cur.fetchone()[0]))
    return counts


def run(directory: Path = SQL_DIR) -> int:
    """Run every script in order. Returns a process exit code.

    Stops at the first failure and reports the three states that leaves behind, because "which
    tables are now stale?" is a question nobody can answer from the error alone.
    """
    scripts = discover(directory)
    if not scripts:
        print(f"no .sql files in {directory}")
        return 1

    target = config.warehouse()
    print(f"running {len(scripts)} scripts into {SCHEMA} at {target.safe_dsn()}\n")

    done: list[Path] = []
    with connect(target.dsn()) as conn:
        apply_schema(conn)

        for script in scripts:
            try:
                elapsed = run_script(conn, script)
            except PostgresError as exc:
                remaining = scripts[len(done) + 1 :]
                print(f"  {script.name:<28} FAILED")
                print(f"\n{type(exc).__name__}: {str(exc).strip()}\n")
                print(f"committed before the failure : {[p.name for p in done]}")
                print(f"rolled back                  : {script.name}")
                print(f"never attempted              : {[p.name for p in remaining]}")
                print(
                    "\nEvery table from the first list is fresh. Every table from the third is\n"
                    "whatever the last successful run left, and still queryable, and still\n"
                    "looks fine. Nothing here can tell you which downstream numbers moved."
                )
                return 1

            print(f"  {script.name:<28} ok    {elapsed:6.2f}s")
            done.append(script)

        print(f"\n{SCHEMA} now holds:")
        for table, rows in summarise(conn):
            print(f"  {table:<28} {rows:>9,} rows")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the hand-written Silver scripts, in order.")
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=SQL_DIR,
        help=f"directory of .sql files to run in filename order (default: {SQL_DIR})",
    )
    args = parser.parse_args()
    return run(args.sql_dir)


if __name__ == "__main__":
    raise SystemExit(main())
