"""
Missing data is represented in Arrow & Polars with a null value.

This null missing value applies for all data types including
numerical values.

Polars also allows NotaNumber or NaN values for float columns.
These NaN values are considered to be a type of floating point
data rather than missing data. We discuss NaN values separately
below.
"""

import polars as pl

df = pl.DataFrame(
    {
        "value": [1, None],
    }
)
print(df)

"""
In Pandas the value for missing data depends on the dtype 
of the column. In Polars missing data is always represented
 as a null value.
"""

null_count_df = df.null_count()
print(null_count_df)

is_null_series = df.select(
    pl.col("value").is_null(),
)
print(is_null_series)

df = pl.DataFrame(
    {
        "col1": [1, 2, 3],
        "col2": [1, None, 3],
    },
)
print(df)

fill_literal_df = df.with_columns(
    pl.col("col2").fill_null(
        pl.lit(2),
    )
)
print(fill_literal_df)

fill_forward_df = df.with_columns(
    pl.col("col2").fill_null(
        strategy="forward",
    )
)
print(fill_forward_df)

fill_median_df = df.with_columns(
    pl.col("col2").fill_null(pl.median("col2")),
)
print(fill_median_df)

fill_interp_df = df.with_columns(
    pl.col("col2").interpolate(),
)
print(fill_interp_df)

import numpy as np

nan_df = pl.DataFrame(
    {
        "value": [1.0, np.NaN, float("nan"), 3.0],
    }
)
print(nan_df)

"""
NaN values are considered to be a type of floating point data and 
are not considered to be missing data in Polars. This means:

- NaN values are not counted with the null_count method
- NaN values are filled when you use fill_nan method but are 
    not filled with the fill_null method

Polars has is_nan and fill_nan methods which work in a similar way 
to the is_null and fill_null methods. The underlying Arrow arrays 
do not have a pre-computed validity bitmask for NaN values so this 
has to be computed for the is_nan method.

One further difference between null and NaN values is that taking
the mean of a column with null values excludes the null values 
from the calculation but with NaN values taking the mean results 
in a NaN. This behaviour can be avoided by replacing the NaN values 
with null values;
"""

mean_nan_df = nan_df.with_columns(
    pl.col("value").fill_nan(None).alias("value"),
).mean()
print(mean_nan_df)
