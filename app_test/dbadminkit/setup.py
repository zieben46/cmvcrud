# setup.py
from setuptools import setup, find_packages

setup(
    name="dbadminkit",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.4.0",
        "psycopg2-binary>=2.9.6",
        "argparse"  # Already in stdlib, but listed for clarity
    ],
    entry_points={
        "console_scripts": [
            "dbadminkit = dbadminkit.cli:main"
        ]
    }
)