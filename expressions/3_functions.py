import polars as pl
import numpy as np

df = pl.DataFrame(
    {
        "nrs": [1, 2, 3, None, 5],
        "names": ["foo", "ham", "spam", "egg", "spam"],
        "random": np.random.rand(5),
        "groups": ["A", "A", "B", "C", "B"],
    }
)
print(df)

# Column Naming
df_samename = df.select(pl.col("nrs") + 5)
print(df_samename)

try:
    df_samename2 = df.select(pl.col("nrs") + 5, pl.col("nrs") - 5)
    print(df_samename2)
except Exception as e:
    print(e)

df_alias = df.select(
    (pl.col("nrs") + 5).alias("nrs + 5"),
    (pl.col("nrs") - 5).alias("nrs - 5"),
)
print(df_alias)

df_map_alias = df.select(pl.all().map_alias(lambda x: x + "_foo"))
print(df_map_alias)

# Count Unique Values
df_alias = df.select(
    pl.col("names").n_unique().alias("unique"),
    pl.approx_unique("names").alias("unique_approx"),
)
print(df_alias)

# Conditionals
df_conditional = df.select(
    pl.col("nrs"), pl.when(pl.col("nrs") > 2).then(pl.lit(True)).otherwise(pl.lit(False)).alias("conditional")
)
print(df_conditional)
