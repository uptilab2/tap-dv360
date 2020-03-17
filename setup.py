#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-dv360",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_dv360"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
        "google-api-python-client",
    ],
    entry_points="""
    [console_scripts]
    tap-dv360=tap_dv360:main
    """,
    packages=["tap_dv360"],
    package_data = {
        "schemas": ["tap_dv360/schemas/*.json"]
    },
    include_package_data=True,
)
