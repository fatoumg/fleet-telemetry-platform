"""Aviation Conflict Analytics platform.

Measures whether geopolitical violence changes commercial aviation behaviour, isolating
weather and day-of-week as confounders. See docs/superpowers/specs/ for the design.

This package holds only the Extract/Load half of the pipeline. Every business rule lives in
dbt (dbt/models/) so it stays diffable and testable as SQL.
"""
