"""Guards on configuration resolution.

Three properties matter here and none is visible from reading the happy path:

  * precedence -- an environment variable must beat the built-in default, or a CI override
    silently does nothing;
  * defaults are honest -- a value that fell back must report itself as defaulted, because a
    default password outside local development is a finding, not a detail;
  * secrets never leak -- `describe()` output lands in terminals and CI logs, and a DSN
    carries a password in the middle of a URL where it is easy to print by accident.

Offline and hermetic: every test passes an explicit env mapping, so the developer's real
`.env` can neither influence a result nor be read by the suite.
"""

from __future__ import annotations

import pytest

from fleet_telemetry import config

# ------------------------------------------------------------------------------------
# defaults and precedence
# ------------------------------------------------------------------------------------


def test_everything_defaults_on_an_empty_environment():
    """A fresh clone with no .env must work -- nothing to configure before the project runs."""
    assert config.oltp({}).database == "fleet"
    assert config.warehouse({}).database == "telemetry"
    assert config.kafka({}).bootstrap_servers == config.KAFKA_DEFAULT_BOOTSTRAP
    assert config.api_token({}) == config.API_TOKEN_DEFAULT
    assert config.api_url({}) == config.API_URL_DEFAULT


def test_api_url_is_overridable():
    """The compose simulator addresses the app by service name, not through the host mapping."""
    assert config.api_url({"FLEET_API_URL": "http://api:8000"}) == "http://api:8000"


def test_env_var_beats_default():
    db = config.warehouse({"WAREHOUSE_HOST": "db.example.test", "WAREHOUSE_PORT": "6000"})
    assert (db.host, db.port) == ("db.example.test", 6000)


def test_blank_value_counts_as_unset():
    """A hand-edited .env easily ends up with an empty value; that means unset, not ''."""
    db = config.warehouse({"WAREHOUSE_USER": "   "})
    assert db.user == "telemetry"
    assert "user" in db.defaulted


def test_partial_override_keeps_other_defaults():
    db = config.oltp({"OLTP_PASSWORD": "explicit"})
    assert db.password == "explicit"
    assert db.host == "127.0.0.1"
    assert db.defaulted == frozenset({"host", "port", "database", "user"})


def test_the_two_databases_are_distinct():
    """Different name, user and port, so a misdirected connection fails loudly."""
    op, wh = config.oltp({}), config.warehouse({})
    assert op.database != wh.database
    assert op.user != wh.user
    assert op.port != wh.port


def test_non_numeric_port_raises():
    with pytest.raises(ValueError, match="WAREHOUSE_PORT"):
        config.warehouse({"WAREHOUSE_PORT": "not-a-port"})


@pytest.mark.parametrize(
    ("var", "value", "attribute"),
    [
        ("WAREHOUSE_HOST", "h.example.test", "host"),
        ("WAREHOUSE_DB", "somedb", "database"),
        ("WAREHOUSE_USER", "someuser", "user"),
        ("WAREHOUSE_PASSWORD", "somepassword", "password"),
    ],
)
def test_env_var_names_match_the_rest_of_the_project(var, value, attribute):
    """Pin the exact variable names docker-compose.yml and dbt/profiles.yml use.

    `WAREHOUSE_DB` is the one that bites: deriving the name from the field would give
    `WAREHOUSE_DATABASE`, so the value everything else sets would be silently ignored and
    dbt and Python would connect to different databases. That shipped once.
    """
    db = config.warehouse({var: value})
    assert getattr(db, attribute) == value
    assert attribute not in db.defaulted, f"{var} was not picked up"


# ------------------------------------------------------------------------------------
# defaults report themselves
# ------------------------------------------------------------------------------------


def test_defaulted_fields_are_tracked():
    assert config.warehouse({}).defaulted == frozenset(config.WAREHOUSE_DEFAULTS)
    fully_set = {config.env_var_for("WAREHOUSE", field): "x" for field in config.WAREHOUSE_DEFAULTS}
    fully_set["WAREHOUSE_PORT"] = "5432"
    assert config.warehouse(fully_set).defaulted == frozenset()


def test_describe_flags_defaults_as_not_explicitly_configured():
    status = {name: explicit for name, explicit, _ in config.describe({})}
    assert status == {
        "OLTP": False,
        "Warehouse": False,
        "Kafka": False,
        "API token": False,
        "API URL": False,
    }


def test_describe_marks_explicit_configuration():
    env = {config.env_var_for("OLTP", f): v for f, v in config.OLTP_DEFAULTS.items()}
    status = {name: explicit for name, explicit, _ in config.describe(env)}
    assert status["OLTP"] is True, "explicitly set values must not report as defaulted"
    assert status["Warehouse"] is False


# ------------------------------------------------------------------------------------
# secrets never leak
# ------------------------------------------------------------------------------------


def test_mask_hides_content_but_reports_length():
    masked = config.mask("supersecretvalue")
    assert "supersecretvalue" not in masked
    assert "16" in masked


def test_mask_handles_missing():
    assert config.mask(None) == "MISSING"


def test_safe_dsn_redacts_the_password():
    db = config.warehouse({"WAREHOUSE_PASSWORD": "hunter2-must-not-appear"})
    assert "hunter2-must-not-appear" not in db.safe_dsn()
    assert "***" in db.safe_dsn()
    # and the real DSN still works, since that is the whole point of having both
    assert "hunter2-must-not-appear" in db.dsn()


def test_dsn_escapes_special_characters_in_credentials():
    """An unescaped @ or / in a password silently produces a DSN pointing somewhere else."""
    db = config.warehouse({"WAREHOUSE_USER": "a/b", "WAREHOUSE_PASSWORD": "p@ss:word"})
    assert "p%40ss%3Aword" in db.dsn()
    assert "a%2Fb" in db.dsn()


def test_describe_never_contains_a_secret():
    """The single most important test in this file."""
    secrets = {
        "OLTP_PASSWORD": "oltp-pw-must-not-appear",
        "WAREHOUSE_PASSWORD": "warehouse-pw-must-not-appear",
        "FLEET_API_TOKEN": "api-token-must-not-appear",
    }
    rendered = " ".join(detail for _, _, detail in config.describe(secrets))
    for value in secrets.values():
        assert value not in rendered, f"{value!r} leaked into describe() output"


def test_main_output_never_contains_a_secret(capsys, monkeypatch):
    """describe() is safe, but main() formats it -- cover the printing path too."""
    for name, value in {
        "OLTP_PASSWORD": "oltp-pw-must-not-appear",
        "WAREHOUSE_PASSWORD": "warehouse-pw-must-not-appear",
        "FLEET_API_TOKEN": "api-token-must-not-appear",
    }.items():
        monkeypatch.setenv(name, value)

    config.main()
    printed = capsys.readouterr().out
    for value in (
        "oltp-pw-must-not-appear",
        "warehouse-pw-must-not-appear",
        "api-token-must-not-appear",
    ):
        assert value not in printed


def test_kafka_settings_default_and_override():
    """The consumer group is a real configured object, not a literal buried in the consumer.

    Rewinding a group to replay bronze means naming it, so it belongs with everything else that
    knows where settings come from.
    """
    default = config.kafka({})
    assert default.consumer_group == config.KAFKA_DEFAULT_GROUP
    assert default.connect_url == config.CONNECT_DEFAULT_URL
    assert default.defaulted == frozenset({"bootstrap_servers", "consumer_group", "connect_url"})

    explicit = config.kafka(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "KAFKA_CONSUMER_GROUP": "replay-2026-08-11",
            "DEBEZIUM_CONNECT_URL": "http://connect:8083",
        }
    )
    assert explicit.consumer_group == "replay-2026-08-11"
    assert explicit.defaulted == frozenset()
