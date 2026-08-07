"""Fleet Telemetry Platform.

An end-to-end data platform built to learn data engineering: an application that produces
events, change data capture out of its database, and a medallion warehouse that must stay
correct while events arrive late, out of order, and in bursts.

See docs/superpowers/specs/ for the design. The domain -- vehicle telemetry for informal
transport in The Gambia -- is grounding, not a product.

This package holds the Extract/Load half only. Every business rule lives in dbt
(dbt/models/) so it stays diffable and testable as SQL.
"""
