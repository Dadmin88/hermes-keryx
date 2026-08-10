from keryx.config import KeryxConfig


def test_daemon_token_loads_from_prefixed_environment() -> None:
    config = KeryxConfig.from_env(
        {
            "HERMES_KERYX_DAEMON_ENDPOINT": "127.0.0.1:50051",
            "HERMES_KERYX_DAEMON_TOKEN": "  unified-token  ",
        }
    )
    assert config.daemon_token == "unified-token"


def test_daemon_token_alias_is_supported() -> None:
    config = KeryxConfig.from_env({"KERYX_DAEMON_TOKEN": "alias-token"})
    assert config.daemon_token == "alias-token"
