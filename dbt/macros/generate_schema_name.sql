{#-
    Layer names are ABSOLUTE, and only this file makes them so.

    Without this override dbt's built-in generate_schema_name applies, and it CONCATENATES:

        {%- if custom_schema_name is none -%} {{ default_schema }}
        {%- else -%} {{ default_schema }}_{{ custom_schema_name | trim }}

    dbt/profiles.yml:29 sets `schema: silver` and dbt/dbt_project.yml:20 sets `+schema: silver`,
    so every staging model resolved to `silver_silver`. MEASURED, before this file existed:

        MODEL stg_depots -> schema: silver_silver
                            relation: "telemetry"."silver_silver"."stg_depots"

    And gold would have been `silver_gold`, marts `silver_marts`.

    WHY THIS WAS WORTH FINDING RATHER THAN TRIPPING OVER. `dbt build` would have reported
    success. The models would have been correct, the tests would have passed against them, and
    the `silver` schema would have stayed empty -- so the failure surfaces later, somewhere else,
    as "why is silver empty" or as a mart reading a schema nobody writes.

    That is this project's recurring failure class, not a dbt quirk: the tool says ok and the
    warehouse disagrees. Same shape as load/schema.py's CREATE TABLE IF NOT EXISTS being
    idempotent against absence rather than divergence, which let schema.py and the database
    disagree about a column while reporting "tables present" and exiting 0
    (docs/silver-by-hand.md, section 4a). Same shape as the four REPLICA IDENTITY FULL statements
    that sat unexecuted for five days because docker/warehouse/init.sql only runs on an empty
    data directory (docs/known-issues.md, section 1).

    Two things it would have broken quietly:

      - The silver_manual-vs-silver diff promised in docs/silver-by-hand.md:314 would have
        compared against a relation that does not exist, or -- worse, once someone created the
        schema by hand -- against an empty one, and reported zero differences.
      - CI pre-creates bronze, silver, gold and marts with psql
        (.github/workflows/ci.yml:83-89) and nothing else. dbt would have created a fifth schema
        nobody declared, in a job whose whole purpose is to prove the SQL runs where it is
        supposed to.

    Verification, if this file is ever suspected:

        python -m dbt.cli.main parse --project-dir dbt --profiles-dir dbt
        python -c "import json,pathlib; m=json.loads(pathlib.Path('dbt/target/manifest.json').read_text(encoding='utf-8')); print(sorted({v['schema'] for v in m['nodes'].values() if v['resource_type']=='model'}))"

    Expect ['silver']. If it says ['silver_silver'], this macro is not being picked up -- check
    the filename and that macro-paths is still ["macros"] (dbt/dbt_project.yml:9).

    The `custom_schema_name is none` branch is kept rather than dropped: a model that declares no
    +schema still needs a home, and target.schema is the right one for it.

    NOT USING generate_schema_name_for_env. That variant routes to target.schema on every target
    except prod, which would put CI's models in `silver` but a developer's in whatever their
    profile says -- reintroducing the same class of surprise from the other direction. Layer
    names here mean the layer, on every target, which is what docker/warehouse/init.sql:20-40
    declares.
-#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
