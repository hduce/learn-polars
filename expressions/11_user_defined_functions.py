"""
User Defined functions
You should be convinced by now that polar expressions 
are so powerful and flexible that there is much less need 
for custom python functions than in other libraries.

Still, you need to have the power to be able to pass an 
expression's state to a third party library or apply 
your black box function over data in polars.

For this we provide the following expressions:
* map
* apply
"""

"""
To map or to apply.
These functions have an important distinction in how they 
operate and consequently what data they will pass to the 
user.

A map passes the Series backed by the expression as is.

map follows the same rules in both the select and the 
groupby context, this will mean that the Series 
represents a column in a DataFrame. Note that in the 
groupby context, that column is not yet aggregated!

Use cases for map are for instance passing the Series in
an expression to a third party library. Below we show 
how we could use map to pass an expression column to 
a neural network model.
"""

import polars as pl

df = pl.DataFrame({"features": [1, 2, 3]})


class MyNeuralNetwork:
    @staticmethod
    def forward(s: pl.Series):
        pass


df.with_columns(
    pl.col("features")
    .map(
        lambda s: MyNeuralNetwork.forward(s.to_numpy),
    )
    .alias("activations"),
)

"""
Use cases for map in the groupby context are slim. They 
are only used for performance reasons, but can quite 
easily lead to incorrect results. Let me explain why...
"""
df = pl.DataFrame(
    {
        "keys": ["a", "a", "b"],
        "values": [10, 7, 1],
    }
)
out = df.groupby("keys", maintain_order=True).agg(
    pl.col("values").map(lambda s: s.shift()).alias("shift_map"),
    pl.col("values").shift().alias("shift_expression"),
)
print(df)
print(out)

"""
In the snippet above we groupby the "keys" column. That means we have the following groups:

"a" -> [10, 7]
"b" -> [1]
If we would then apply a shift operation to the right, we'd expect:

"a" -> [null, 10]
"b" -> [null]

shape: (2, 3)
┌──────┬────────────┬──────────────────┐
│ keys ┆ shift_map  ┆ shift_expression │
│ ---  ┆ ---        ┆ ---              │
│ str  ┆ list[i64]  ┆ list[i64]        │
╞══════╪════════════╪══════════════════╡
│ a    ┆ [null, 10] ┆ [null, 10]       │
│ b    ┆ [7]        ┆ [null]           │
└──────┴────────────┴──────────────────┘
Ouch.. we clearly get the wrong results here. Group "b" even got a value from group "a" 😵.

This went horribly wrong, because the map applies the function before we aggregate! So that means the whole column [10, 7, 1] got shifted to [null, 10, 7] and was then aggregated.

So my advice is to never use map in the groupby context unless you know you need it and know what you are doing.
"""
