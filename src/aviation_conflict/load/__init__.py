"""Bronze files -> bronze.* raw tables in the warehouse.

Loads the captured JSON as-is (JSONB) with no reshaping: files are the durable record, tables
make them queryable. Parsing and typing happen in dbt, not here.
"""
