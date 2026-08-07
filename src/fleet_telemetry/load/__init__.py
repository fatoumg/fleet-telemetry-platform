"""Bronze -> bronze.* raw tables in the warehouse.

Loads captured events as-is (JSONB) with no reshaping. Parsing, typing and deduplication all
happen in dbt, not here -- this layer's only job is to make the durable record queryable.

Deliberately tolerant: a payload that fails to parse is still stored, with the parse error
recorded alongside it. Rejecting it at this boundary would delete the evidence needed to
diagnose why it was malformed.
"""
