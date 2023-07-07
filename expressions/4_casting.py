"""
Casting converts the underlying DataType of a column to 
a new one. Polars uses Arrow to manage the data in memory 
and relies on the compute kernels in the rust implementation 
to do the conversion. Casting is available with the cast() method.

The cast method includes a strict parameter that determines 
how Polars behaves when it encounters a value that can't be 
converted from the source DataType to the target DataType. 
By default, strict=True, which means that Polars will throw 
an error to notify the user of the failed conversion and 
provide details on the values that couldn't be cast. 
On the other hand, if strict=False, any values that can't 
be converted to the target DataType will be quietly converted 
to null.
"""
import polars as pl

# Numerics
df = pl.DataFrame(
    {
        "integers": [1, 2, 3, 4, 5],
        "big_integers": [1, 10000002, 3, 10000004, 10000005],
        "floats": [4.0, 5.0, 6.0, 7.0, 8.0],
        "floats_with_decimal": [4.532, 5.5, 6.5, 7.5, 8.5],
    }
)
print(df)

out = df.select(
    pl.col("integers").cast(pl.Float32).alias("integers_as_floats"),
    pl.col("floats").cast(pl.Int32).alias("floats_as_integers"),
    pl.col("floats_with_decimal").cast(pl.Int32).alias("floats_with_decimal_integers"),
)
print(out)

out = df.select(
    pl.col("integers").cast(pl.Int16).alias("integers_smallfootprint"),
    pl.col("floats").cast(pl.Float32).alias("floats_smallfootprint"),
)
print(out)

try:
    out = df.select(pl.col("big_integers").cast(pl.Int8))
    print(out)
except Exception as e:
    print(e)

out = df.select(pl.col("big_integers").cast(pl.Int8, strict=False))
print(out)

# Strings

df = pl.DataFrame(
    {
        "integers": [1, 2, 3, 4, 5],
        "float": [4.0, 5.03, 6.0, 7.0, 8.0],
        "floats_as_string": ["4.0", "5.0", "6.0", "7.0", "8.0"],
    }
)
out = df.select(
    pl.col("integers").cast(pl.Utf8),
    pl.col("float").cast(pl.Utf8),
    pl.col("floats_as_string").cast(pl.Float64),
)
print(out)

df = pl.DataFrame({"strings_not_float": ["4.0", "not_a_number", "6.0", "7.0"]})
try:
    out = df.select(pl.col("strings_not_float").cast(pl.Float64))
    print(out)
except Exception as e:
    print(e)

# Booleans
df = pl.DataFrame(
    {
        "integers": [-1, 0, 2, 3, 4],
        "floats": [0.0, 1.0, 2.0, 3.0, 4.0],
        "bools": [True, False, True, False, True],
    }
)
out = df.select(pl.col("integers").cast(pl.Boolean), pl.col("floats").cast(pl.Boolean))
print(out)

from datetime import date, datetime

df = pl.DataFrame(
    {
        "date": pl.date_range(date(2022, 1, 1), date(2022, 1, 5), eager=True),
        "datetime": pl.date_range(
            datetime(2022, 1, 1), datetime(2022, 1, 5), eager=True
        ),
    }
)
out = df.select(pl.col("date").cast(pl.Int64), pl.col("datetime").cast(pl.Int64))
print(out)

df = pl.DataFrame(
    {
        "date": pl.date_range(date(2022, 1, 1), date(2022, 1, 5), eager=True),
        "string": [
            "2022-01-01",
            "2022-01-02",
            "2022-01-03",
            "2022-01-04",
            "2022-01-05",
        ],
    }
)

out = df.select(
    pl.col("date").dt.strftime("%Y-%m-%d"),
    pl.col("string").str.strptime(pl.Datetime, "%Y-%m-%d"),
)
print(out)
