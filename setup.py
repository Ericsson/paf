# -*- coding: utf-8 -*-

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="paf",
    version="0.1.0",
    author="Mattias Rönnblom",
    author_email="mattias.ronnblom@ericsson.com",
    description="The Pathfinder Service Discovery Server",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Ericsson/paf",
    packages=setuptools.find_packages(),
    scripts=['app/pafd', 'app/pafc', 'app/pafbench'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: Linux",
    ],
    python_requires='>=3.5'
)
