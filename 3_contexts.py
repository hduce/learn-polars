"""
A context, as implied by the name, refers to the context in which an expression needs to be evaluated. 
There are three main contexts:

- Selection: df.select([..]), df.with_columns([..])
- Filtering: df.filter()
- Groupby / Aggregation: df.groupby(..).agg([..])
"""
import numpy as np
import polars as pl

df = pl.DataFrame(
    {
        "nrs": [1, 2, 3, None, 5],
        "names": ["foo", "ham", " spam", "egg", None],
        "random": np.random.rand(5),
        "groups": ["A", "A", "B", "C", "B"],
    }
)

print(df)

"""
.select creates a new dataframe dropping all columns except those specified in the context

"""
out = df.select(
    pl.sum("nrs"),
    pl.col("names").sort(),
    pl.col("names").first().alias("first name"),
    (pl.mean("nrs") * 10).alias("10xnrs"),
)
print(out)

"""
.with_columns creates a new dataframe retaining all the existing columns from
the previous df.
"""
df = df.with_columns(
    pl.sum("nrs").alias("nrs_sum"),
    pl.col("random").count().alias("count"),
)
print(df)

out = df.filter(pl.col("nrs") > 2)
print(out)

out = df.groupby("groups").agg(
    pl.sum("nrs"),  # sum nrs by groups
    pl.col("random").count().alias("count"),  # count group members
    # sum random where name != null
    pl.col("random").filter(pl.col("names").is_not_null()).sum().suffix("_sum"),
    pl.col("names").reverse().alias("reversed names"),
)
print(out)
