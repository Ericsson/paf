def pytest_addoption(parser):
    parser.addoption("--server", action="store", default="paf",
                     help="Run against this server type (paf or tpaf)")
    parser.addoption("--server-debug", action="store_true", default=False,
                     help="Enable server debug logging")
