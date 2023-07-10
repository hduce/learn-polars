"""
Polars provides expressions/methods for horizontal 
aggregations like sum,min, mean, etc. by setting the 
argument axis=1. However, when you need a more complex 
aggregation the default methods Polars supplies may not be sufficient. That's when folds come in handy.

The fold expression operates on columns for maximum speed. 
It utilizes the data layout very efficiently and often has 
vectorized execution.
"""

import polars as pl

df = pl.DataFrame(
    {
        "a": [1, 2, 3],
        "b": [10, 20, 30],
        # "c": ["foo", "bar", "spam"],
    }
)

out = df.select(
    pl.fold(
        acc=pl.lit(0),
        function=lambda acc, x: acc + x,
        exprs=pl.all(),
    ).alias("sum")
)
print(out)
"""
The snippet above recursively applies the function 
f(acc, x) -> acc to an accumulator acc and a new 
column x. The function operates on columns individually 
and can take advantage of cache efficiency and vectorization.
"""

df = pl.DataFrame(
    {
        "a": [1, 2, 3],
        "b": [4, 1, 2],
    }
)

"""
Apply a predicate over all columns. 
The predicate here being > 1.
In the snippet we filter all rows where each column 
    value is > 1.
"""
out = df.filter(
    pl.fold(
        acc=pl.lit(True),
        function=lambda acc, x: acc & x,
        exprs=pl.col("*") > 1,
    )
)
print(out)

"""
Folds could be used to concatenate string data. 
However, due to the materialization of intermediate columns,
this operation will have squared complexity.

Therefore, we recommend using the concat_str expression 
for this.
"""
df = pl.DataFrame(
    {
        "a": ["a", "b", "c"],
        "b": [1, 2, 3],
    }
)
out = df.select(pl.concat_str(["a", "b"]))
print(out)
