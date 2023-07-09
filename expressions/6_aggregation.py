import polars as pl

url = "https://theunitedstates.io/congress-legislators/legislators-historical.csv"

dtypes = {
    "first_name": pl.Categorical,
    "gender": pl.Categorical,
    "type": pl.Categorical,
    "state": pl.Categorical,
    "party": pl.Categorical,
}

dataset = (
    pl.read_csv(url, dtypes=dtypes)
    .with_columns(pl.col("birthday").str.strptime(pl.Date, strict=False))
    .with_row_count("id")
)
print(dataset)

"""
Basic aggregations
You can easily combine different aggregations by adding multiple expressions in a list. There is no upper bound on the number of aggregations you can do, and you can make any combination you want. In the snippet below we do the following aggregations:

Per GROUP "first_name" we:
- count the number of rows in the group:
- short form: pl.count("party")
- full form: pl.col("party").count()
- aggregate the gender values groups:
- full form: pl.col("gender")
- get the first value of column "last_name" in the group:
- short form: pl.first("last_name") (not available in Rust)
- full form: pl.col("last_name").first()
"""


q = (
    dataset.lazy()
    .groupby("first_name")
    .agg(
        pl.count(),
        pl.col("gender"),
        pl.first("last_name"),
    )
    .sort("count", descending=True)
    .limit(5)
)
df = q.collect()
print(df)

q = (
    dataset.lazy()
    .groupby("state")
    .agg(
        (pl.col("party") == "Anti-Administration").sum().alias("anti"),
        (pl.col("party") == "Pro-Administration").sum().alias("pro"),
    )
    .sort("pro", descending=True)
    .limit(5)
)
df = q.collect()
print(df)

# Equiv to above query
q = (
    dataset.lazy()
    .groupby("state", "party")
    .agg(
        pl.count("party").alias("count"),
    )
    .filter(
        (pl.col("party") == "Anti-Administration")
        | (pl.col("party") == "Pro-Administration")
    )
    .sort("count", descending=True)
    .limit(5)
)
df = q.collect()
print(df)

"""
Filtering
We can also filter the groups. Let's say we want to compute 
a mean per group, but we don't want to include all values 
from that group, and we also don't want to filter the rows 
from the DataFrame (because we need those rows for another aggregation).

In the example below we show how this can be done.
"""
from datetime import date

# Expr is Fn(Series) -> Series where the output Series len
# is either 1 or the len of the input series


def compute_age() -> pl.Expr:
    return date(2021, 1, 1).year - pl.col("birthday").dt.year()


def avg_birthday(gender: str) -> pl.Expr:
    return (
        compute_age()
        .filter(pl.col("gender") == gender)
        .mean()
        .alias(f"avg {gender} birthday")
    )


q = (
    dataset.lazy()
    .groupby("state")
    .agg(
        avg_birthday("M"),
        avg_birthday("F"),
        (pl.col("gender") == "M").sum().alias("# male"),
        (pl.col("gender") == "F").sum().alias("# female"),
    )
    .limit(5)
)

df = q.collect()
print(df)


def get_person() -> pl.Expr:
    return pl.col("first_name") + pl.lit(" ") + pl.col("last_name")


q = (
    dataset.lazy()
    .sort("birthday", descending=True)
    .groupby("state")
    .agg(
        get_person().first().alias("youngest"),
        get_person().last().alias("oldest"),
    )
    .limit(5)
)

df = q.collect()
print(df)

q = (
    dataset.lazy()
    .sort("birthday", descending=True)
    .groupby("state")
    .agg(
        get_person().first().alias("youngest"),
        get_person().last().alias("oldest"),
        get_person().sort().first().alias("alphabetical_first"),
    )
    .limit(5)
)
df = q.collect()
print(df)


q = (
    dataset.lazy()
    .sort("birthday", descending=True)
    .groupby("state")
    .agg(
        get_person().first().alias("youngest"),
        get_person().last().alias("oldest"),
        get_person().sort().first().alias("alphabetical_first"),
        pl.col("gender").sort_by("first_name").first().alias("gender"),
    )
    .sort("state")
    .limit(5)
)

df = q.collect()
print(df)
