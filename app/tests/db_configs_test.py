import pytest
from app.config.db_configs import PostgresConfig

def test_should_throw_error_when_no_credentials_to_load():
    getenv_function = lambda var: None
    postgres_config = PostgresConfig(getenv_function)
    with pytest.raises(ValueError):
        postgres_config._load_credentials()

def test_build_URL_expected_value():
    getenv_function = lambda var: "999" if var == "POSTGRES_PORT" else str(var)
    postgres_config = PostgresConfig(getenv_function)
    result = str(postgres_config.get_url())
    expected = "POSTGRES_DRIVERNAME://POSTGRES_USERNAME:***@POSTGRES_HOST:999/POSTGRES_DATABASE"
    assert result == expected
