import polars as pl

# then let's load some csv data with information about pokemon
df = pl.read_csv(
    "https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv"
)
print(df.head())

"""
Groupby Aggregations in selection
Below we show how to use window functions to group over 
different columns and perform an aggregation on them. 
Doing so allows us to use multiple groupby operations 
in parallel, using a single query. The results of the 
aggregation are projected back to the original rows. 
Therefore, a window function will almost always lead to 
a DataFrame with the same size as the original.

We will discuss later the cases where a window function 
can change the numbers of rows in a DataFrame.

Note how we call .over("Type 1") and 
.over(["Type 1", "Type 2"]). Using window functions 
we can aggregate over different groups in a single select call! 
Note that, in Rust, the type of the argument to over() 
must be a collection, so even when you're only using 
one column, you must provided it in an array.

The best part is, this won't cost you anything. The 
computed groups are cached and shared between different 
window expressions.
"""
out = df.select(
    "Type 1",
    "Type 2",
    pl.col("Attack").mean().over("Type 1").alias("avg_attack_by_type"),
    pl.col("Defense")
    .mean()
    .over(["Type 1", "Type 2"])
    .alias("avg_defense_by_type_combination"),
    pl.col("Attack").mean().alias("avg_attack"),
)
print(out)

filtered = df.filter(pl.col("Type 2") == "Psychic").select(
    "Name",
    "Type 1",
    "Speed",
)
print(filtered)

out = filtered.with_columns(
    pl.col(["Name", "Speed"]).sort_by("Speed", descending=True).over("Type 1")
)
print(out)

"""
The power of window expressions is that you often don't 
need a groupby -> explode combination, but you can put 
the logic in a single expression. It also makes the API 
cleaner. If properly used a:

* groupby -> marks that groups are aggregated and we 
    expect a DataFrame of size n_groups
* over -> marks that we want to compute something 
    within a group, and doesn't modify the original size 
    of the DataFrame except in specific cases
    
In cases where the expression results in multiple values per 
group, the Window function has 3 strategies for linking the 
values back to the DataFrame rows:
- mapping_strategy = 'group_to_rows' -> each value is 
    assigned back to one row. The number of values 
    returned should match the number of rows.
- mapping_strategy = 'join' -> the values are imploded 
    in a list, and the list is repeated on all rows. 
    This can be memory intensive.
- mapping_strategy = 'explode' -> the values are exploded 
    to new rows. This operation changes the number of rows.
"""

# aggregate and broadcast within a group
# output type: -> Int32
pl.sum("foo").over("groups")

# sum within a group and multiply with group elements
# output type: -> Int32
(pl.col("x").sum() * pl.col("y")).over("groups")

# sum within a group and multiply with group elements
# and aggregate the group to a list
# output type: -> List(Int32)
(pl.col("x").sum() * pl.col("y")).over("groups", mapping_strategy="join")

# sum within a group and multiply with group elements
# and aggregate the group to a list
# then explode the list to multiple rows

# This is the fastest method to do things over groups when the groups are sorted
(pl.col("x").sum() * pl.col("y")).over("groups", mapping_strategy="explode")

out = df.sort("Type 1").select(
    pl.col("Type 1").head(3).over("Type 1", mapping_strategy="explode"),
    pl.col("Name")
    .sort_by(pl.col("Speed"))
    .head(3)
    .over("Type 1", mapping_strategy="explode")
    .alias("fastest/group"),
    pl.col("Name")
    .sort_by(pl.col("Attack"))
    .head(3)
    .over("Type 1", mapping_strategy="explode")
    .alias("strongest/group"),
    pl.col("Name")
    .sort()
    .head(3)
    .over("Type 1", mapping_strategy="explode")
    .alias("sorted_by_alphabet"),
)
print(out)

"""
Windowing the data over a specified group.

E.g. windowing over a column of dates would group data
where the dates match.

The mapping strat determines how the output is mapped back
to the df. Using strat group_to_rows, if there are multiple
rows in the original df with the same val for the windowed
column, then the derived value is mapped across all those
rows. 
"""
