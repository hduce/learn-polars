from pathlib import Path

import polars as pl

f_name = Path(__file__).parent.parent / "data" / "iris.csv"
"""
This is the Eager API.

Steps:
1. Read the iris dataset.
2. Filter the dataset based on sepal length
3. Calculate the mean of the sepal width per species

Inefficient because:
* Predicate pushdown: Apply filters as early as possible 
    while reading the dataset, thus only reading rows with sepal length greater than 5.
* Projection pushdown: Select only the columns that are
    needed while reading the dataset, thus removing the need to load additional columns (e.g. petal length & petal width)
"""
df = pl.read_csv(f_name)
print(df.schema)
df_small = df.filter(pl.col("sepal_length") > 5)
df_agg = df_small.groupby("species").agg(pl.col("sepal_width").mean())
print(df_agg)

"""
This is the lazy API

These will significantly lower the load on memory & 
CPU thus allowing you to fit bigger datasets in memory 
and process faster. Once the query is defined you call 
collect to inform Polars that you want to execute it.
"""

q = pl.scan_csv(f_name).filter(pl.col("sepal_length") > 5).groupby("species").agg(pl.col("sepal_width").mean())

df = q.collect()
print(df)

"""
When to use which:
    In general the lazy API should be preferred unless 
    you are either interested in the intermediate 
    results or are doing exploratory work and don't
    know yet what your query is going to look like.
"""
