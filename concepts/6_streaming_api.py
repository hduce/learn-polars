from pathlib import Path
import polars as pl

f_name = Path(__file__).parent.parent / "data" / "iris.csv"

"""
Instead of processing the data all-at-once Polars 
can execute the query in batches allowing you to 
process datasets that are larger-than-memory.
"""
q = pl.scan_csv(f_name).filter(pl.col("sepal_length") > 5).groupby("species").agg(pl.col("sepal_width").mean())

df = q.collect(streaming=True)
print(df)
