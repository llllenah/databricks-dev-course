# Databricks notebook source
# MAGIC %pip install -q pytest

# COMMAND ----------

import os
import sys

import pytest

project_root = os.path.abspath("..")
os.environ["SPARK_MODE"] = "databricks"
sys.dont_write_bytecode = True
os.chdir(project_root)

exit_code = pytest.main(["-q", "-p", "no:cacheprovider", "tests"])
if exit_code != 0:
    raise Exception(f"unit tests failed, pytest exit code {exit_code}")
