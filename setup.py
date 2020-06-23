# -*- coding: utf-8 -*-

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="paf",
    version="0.0.1",
    author="Mattias Rönnblom",
    author_email="mattias.ronnblom@ericsson.com",
    description="The Pathfinder Service Discovery Server",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gerrit.ericsson.se/paf/paf",
    packages=setuptools.find_packages(),
    scripts=['app/pafd', 'app/pafc', 'app/pafbench']
)
