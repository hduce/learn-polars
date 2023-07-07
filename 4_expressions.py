"""
Polars expressions are a mapping from a series to a 
series (or mathematically Fn(Series) -> Series). As expressions
have a Series as an input and a Series as an output then it 
is straightforward to do a sequence of expressions (similar to
method chaining in Pandas).
"""
import polars as pl

pl.col("foo").sort().head(2)

"""
The snippet above says:

1. Select column "foo"
2. Then sort the column (not in reversed order)
3. Then take the first two values of the sorted output

The power of expressions is that every expression produces a new expression, 
and that they can be piped together. You can run an expression by
passing them to one of Polars execution contexts.

Here we run two expressions by running df.select:
"""
df.select(
    pl.col("foo").sort().head(2),
    pl.col("bar").filter(pl.col("foo") == 1).sum(),
)
"""
All expressions are run in parallel, meaning that separate Polars 
expressions are embarrassingly parallel. Note that within an 
expression there may be more parallelization going on.
"""
