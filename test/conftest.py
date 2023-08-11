def pytest_addoption(parser):
    parser.addoption("--server", action="store", default="paf",
                     help="Run against this server type (paf or tpaf)")
