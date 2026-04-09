"""
generate_sales.py — Fake retail sales data generator.

Produces realistic-looking stores, products, and daily sales transactions
that mimic what a tobacco distributor's data sources would emit.

Usage:
    python data_generator/generate_sales.py --days 90
    python data_generator/generate_sales.py --days 1 --append   # daily incremental

The script writes directly into the `raw` schema in Postgres.
"""

import argparse
