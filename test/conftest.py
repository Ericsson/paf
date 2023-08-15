import pytest
from feature import ServerFeature


def pytest_addoption(parser):
    parser.addoption("--server", action="store", default="paf",
                     help="Run against this server type (paf or tpaf)")
    parser.addoption("--server-debug", action="store_true", default=False,
                     help="Enable server debug logging")
    parser.addoption("--server-valgrind", action="store_true", default=False,
                     help="Run the server in valgrind")


def modify_test_paf_item(config, item):
    if config.option.server_valgrind and \
       "skip_in_valgrind" in item.keywords:
        skip_in_valgrind = \
            pytest.mark.skip(reason="not compatible with valgrind")
        item.add_marker(skip_in_valgrind)

    server_name = config.option.server

    for feature in ServerFeature:
        feature_name = feature.name.lower()
        feature_desc = feature_name.replace("_", " ")
        mark_name = "require_%s" % feature_name

        if mark_name not in item.keywords:
            continue

        if item.module.server_supports(server_name, feature):
            continue

        skip_lacking_feature = \
            pytest.mark.skip(reason="no %s support" % feature_desc)

        item.add_marker(skip_lacking_feature)


def pytest_collection_modifyitems(config, items):
    for item in items:
        if item.module.__name__ == "test_paf":
            modify_test_paf_item(config, item)


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "skip_in_valgrind: skip test when server is run in valgrind"
    )

    for feature in ServerFeature:
        feature_name = feature.name.lower()
        feature_desc = feature_name.replace("_", " ")
        mark_name = "require_%s" % feature_name

        config.addinivalue_line(
            "markers", "%s: skip if server lacks support for %s" %
            (mark_name, feature_desc)
        )
