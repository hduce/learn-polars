"""
Polars has first-class support for List columns: that is, 
columns where each row is a list of homogenous elements, 
of varying lengths. Polars also has an Array datatype, 
which is analogous to numpy's ndarray objects, where the 
length is identical across rows.

Note: this is different from Python's list object, where 
the elements can be of any type. Polars can store these 
within columns, but as a generic Object datatype that 
doesn't have the special list manipulation features 
that we're about to discuss.
"""
import polars as pl

weather = pl.DataFrame(
    {
        "station": ["Station " + str(x) for x in range(1, 6)],
        "temperatures": [
            "20 5 5 E1 7 13 19 9 6 20",
            "18 8 16 11 23 E2 8 E2 E2 E2 90 70 40",
            "19 24 E9 16 6 12 10 22",
            "E2 E0 15 7 8 10 E1 24 17 13 6",
            "14 8 E0 16 22 24 E1",
        ],
    }
)
print(weather)

weather_list = weather.select(
    pl.col("station"),
    pl.col("temperatures").str.split(" "),
)
print(weather_list)

intermediate_out = weather_list.explode("temperatures")
print(intermediate_out)

"""
Polars provides several standard operations on List 
columns. If we want the first three measurements, 
we can do a head(3). The last three can be obtained
via a tail(3), or alternately, via slice (negative 
indexing is supported). We can also identify the 
number of observations via lengths. 

Let's see them in action:
"""
intermediate_out = weather_list.with_columns(
    pl.col("temperatures").list.head(3).alias("top3"),
    pl.col("temperatures").list.slice(-3, 3).alias("bottom_3"),
    pl.col("temperatures").list.lengths().alias("obs"),
)
print(intermediate_out)

"""
If we need to identify the stations that are giving the 
most number of errors from the starting DataFrame, we 
need to:
1. Parse the string input as a List of string values 
    (already done).
2. Identify those strings that can be converted to numbers.
3. Identify the number of non-numeric values (i.e. 
    null values) in the list, by row.
4. Rename this output as errors so that we can easily 
    identify the stations.

The third step requires a casting (or alternately, 
a regex pattern search) operation to be perform on 
each element of the list. We can do this using by
applying the operation on each element by first 
referencing them in the pl.element() context, and 
then calling a suitable Polars expression on them. 
Let's see how:
"""

intermediate_out = weather_list.with_columns(
    pl.col("temperatures")
    # Try casting to int. If cannot then val is set to null. Then
    # is_null() checks each val and returns a bool, true if null
    .list.eval(pl.element().cast(pl.Int64, strict=False).is_null())
    # Counts all instances of true in the series
    .list.sum().alias("errors")
)
print(intermediate_out)

# Same but use regex instead of casting!
intermediate_out = weather_list.with_columns(
    pl.col("temperatures")
    .list.eval(
        # uses the 'rust regex create'
        pl.element().str.contains("(?i)[a-z]"),
    )
    .list.sum()
    .alias("errors")
)
print(intermediate_out)

"""
Row-wise computations
This context is ideal for computing in row orientation.

We can apply any Polars operations on the elements of 
the list with the list.eval (list().eval in Rust) 
expression! These expressions run entirely on Polars' 
query engine and can run in parallel, so will be well 
optimized. Let's say we have another set of weather 
data across three days, for different stations:
"""
weather_by_day = pl.DataFrame(
    {
        "station": ["Station " + str(x) for x in range(1, 11)],
        "day_1": [17, 11, 8, 22, 9, 21, 20, 8, 8, 17],
        "day_2": [15, 11, 10, 8, 7, 14, 18, 21, 15, 13],
        "day_3": [16, 15, 24, 24, 8, 23, 19, 23, 16, 10],
    }
)
print(weather_by_day)

"""
Let's do something interesting, where we calculate 
the percentage rank of the temperatures by day, 
measured across stations. Pandas allows you to 
compute the percentages of the rank values. 
Polars doesn't provide a special function to do 
this directly, but because expressions are so 
versatile we can create our own percentage rank 
expression for highest temperature. 

Let's try that!
"""
rank_pct = (pl.element().rank(descending=True) / pl.col("*").count()).round(2)

intermediate_out = weather_by_day.with_columns(
    # create the list of homogeneous data
    pl.concat_list(pl.all().exclude("station")).alias("all_temps")
)
print(intermediate_out)

out = intermediate_out.select(
    # select all columns except the intermediate list
    pl.all().exclude("all_temps"),
    # compute the rank by calling `list.eval`
    # polars magic knows to inspect the whole list when
    # calling pl.element().rank()
    pl.col("all_temps").list.eval(rank_pct, parallel=True).alias("temps_rank"),
)
print(out)

"""
shape: (10, 5)
┌────────────┬───────┬───────┬───────┬────────────────────┐
│ station    ┆ day_1 ┆ day_2 ┆ day_3 ┆ temps_rank         │
│ ---        ┆ ---   ┆ ---   ┆ ---   ┆ ---                │
│ str        ┆ i64   ┆ i64   ┆ i64   ┆ list[f64]          │
╞════════════╪═══════╪═══════╪═══════╪════════════════════╡
│ Station 1  ┆ 17    ┆ 15    ┆ 16    ┆ [0.33, 1.0, 0.67]  │
│ Station 2  ┆ 11    ┆ 11    ┆ 15    ┆ [0.83, 0.83, 0.33] │
│ Station 3  ┆ 8     ┆ 10    ┆ 24    ┆ [1.0, 0.67, 0.33]  │
│ Station 4  ┆ 22    ┆ 8     ┆ 24    ┆ [0.67, 1.0, 0.33]  │
│ …          ┆ …     ┆ …     ┆ …     ┆ …                  │
│ Station 7  ┆ 20    ┆ 18    ┆ 19    ┆ [0.33, 1.0, 0.67]  │
│ Station 8  ┆ 8     ┆ 21    ┆ 23    ┆ [1.0, 0.67, 0.33]  │
│ Station 9  ┆ 8     ┆ 15    ┆ 16    ┆ [1.0, 0.67, 0.33]  │
│ Station 10 ┆ 17    ┆ 13    ┆ 10    ┆ [0.33, 0.67, 1.0]  │
└────────────┴───────┴───────┴───────┴────────────────────┘
"""

"""
Polars Arrays
Arrays are a new data type that was recently introduced, 
and are still pretty nascent in features that it offers. 
The major difference between a List and an Array is that 
the latter is limited to having the same number of 
elements per row, while a List can have a variable number 
of elements. Both still require that each element's data 
type is the same.

We can define Array columns in this manner:
"""
array_df = pl.DataFrame(
    [
        pl.Series("Array_1", [[1, 3], [2, 5]]),
        pl.Series("Array_2", [[1, 7, 3], [8, 1, 0]]),
    ],
    schema={"Array_1": pl.Array(2, pl.Int64), "Array_2": pl.Array(3, pl.Int64)},
)
print(array_df)

out = array_df.select(
    pl.col("Array_1").arr.min().suffix("_min"),
    pl.col("Array_2").arr.sum().suffix("_sum"),
)
print(out)
