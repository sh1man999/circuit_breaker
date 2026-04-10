#!/usr/bin/env python
# -*- coding:utf-8 -*-

from setuptools import find_packages, setup

from aiobreaker import version

with open("readme.rst", "r") as fh:
    long_description = fh.read()

test_dependencies = [
    "fakeredis>=2.20",
    "pytest>=7",
    "pytest-asyncio>=0.23",
    "mypy",
    "ruff",
    "codecov",
    "pytest-cov",
]
documentation_dependencies = [
    "sphinx",
    "sphinx_rtd_theme",
    "sphinx-autobuild",
    "sphinx-autodoc-typehints",
]

setup(
    name="aiobreaker",
    version=version.__version__,
    url="https://github.com/arlyon/aiobreaker",
    license="BSD",
    author="Alexander Lyon",
    author_email="arlyon@me.com",
    description="Asynchronous Python implementation of the Circuit Breaker pattern.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=("test", "tests", "tests.*")),
    py_modules=["aiobreaker"],
    python_requires=">=3.10",
    install_requires=["redis>=4.2"],
    tests_require=test_dependencies,
    extras_require={
        "test": test_dependencies,
        "docs": documentation_dependencies,
    },
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries",
    ],
)
