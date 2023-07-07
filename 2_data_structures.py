import polars as pl

s = pl.Series("a", [1, 2, 3, 4, 5])
print(s)

from datetime import datetime

df = pl.DataFrame(
    {
        "integer": [1, 2, 3, 4, 5],
        "date": [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2),
            datetime(2023, 1, 3),
            datetime(2023, 1, 4),
            datetime(2023, 1, 5),
        ],
        "float": [4.0, 5.0, 6.0, 7.0, 8.0],
    }
)

print(df)

print(df.head(3))

print(df.tail(3))

print(df.sample(3))

print(df.describe())
