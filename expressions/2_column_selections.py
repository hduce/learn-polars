from datetime import date, datetime
import polars as pl

df = pl.DataFrame(
    {
        "id": [9, 4, 2],
        "place": ["Mars", "Earth", "Saturn"],
        "date": pl.date_range(date(2022, 1, 1), date(2022, 1, 3), "1d", eager=True),
        "sales": [33.4, 2142134.1, 44.7],
        "has_people": [False, True, False],
        "logged_at": pl.date_range(datetime(2022, 12, 1), datetime(2022, 12, 1, 0, 0, 2), "1s", eager=True),
    }
).with_row_count("rn")

print(df)
out = df.select(pl.col("*"))

# Is equivalent too
out = df.select(pl.all())
print(out)

out = df.select(pl.col("*").exclude("logged_at", "rn"))
print(out)

out = df.select(pl.col("date", "logged_at").dt.to_string("%Y-%h-%-d"))
print(out)

out = df.select(pl.col("^.*(as|sa).*$"))
print(out)

out = df.select(pl.col(pl.Int64, pl.UInt32, pl.Boolean).n_unique())
print(out)

import polars.selectors as cs

out = df.select(cs.integer(), cs.string())
print(out)

"""
Applying set operations
These selectors also allow for set based selection 
operations. For instance, to select the numeric columns 
except the first column that indicates row numbers:
"""
out = df.select(cs.numeric() - cs.first())
print(out)

"""
We can also select the row number by name and any 
non-numeric columns:
"""
out = df.select(cs.by_name("rn"), ~cs.numeric())
print(out)

out = df.select(cs.contains("rn"), cs.matches(".*_.*"))
print(out)

out = df.select(cs.temporal().as_expr().dt.to_string("%Y-%h-%d"))
print(out)

from polars.selectors import is_selector

out = cs.temporal()
print(is_selector(out))

"""
To predetermine the column names that are selected, which
is especially useful for a LazyFrame object:
"""
from polars.selectors import selector_column_names

out = cs.temporal().as_expr().dt.to_string("%Y-%h-%d")
print(selector_column_names(df, out))
