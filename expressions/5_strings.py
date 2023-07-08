"""
The following section discusses operations performed on 
Utf8 strings, which are a frequently used DataType when 
working with DataFrames. However, processing strings can 
often be inefficient due to their unpredictable memory size, 
causing the CPU to access many random memory locations. 
To address this issue, Polars utilizes Arrow as its backend, 
which stores all strings in a contiguous block of memory. 
As a result, string traversal is cache-optimal and predictable
for the CPU.

String processing functions are available in the str namespace.
"""

import polars as pl

df = pl.DataFrame({"animal": ["Crab", "cat and dog", "rab$bit", None]})

out = df.select(
    pl.col("animal").str.lengths().alias("byte_count"),
    pl.col("animal").str.n_chars().alias("letter_count"),
)
print(out)

out = df.select(
    pl.col("animal"),
    pl.col("animal").str.contains("cat|bit").alias("regex"),
    pl.col("animal").str.contains("rab$", literal=True).alias("literal"),
    pl.col("animal").str.starts_with("rab").alias("starts_with"),
    pl.col("animal").str.ends_with("dog").alias("ends_with"),
)
print(out)

"""
EXTRACT A PATTERN
The extract method allows us to extract a pattern from
a specified string. This method takes a regex pattern 
containing one or more capture groups, which are defined 
by parentheses () in the pattern. The group index indicates
which capture group to output.
"""
df = pl.DataFrame(
    {
        "a": [
            "http://vote.com/ballon_dor?candidate=messi&ref=polars",
            "http://vote.com/ballon_dor?candidat=jorginho&ref=polars",
            "http://vote.com/ballon_dor?candidate=ronaldo&ref=polars",
        ]
    }
)
out = df.select(
    pl.col("a").str.extract(r"candidate=(\w+)", group_index=1),
)
print(out)

df = pl.DataFrame({"foo": ["123 bla 45 asd", "xya 678 910t"]})
out = df.select(pl.col("foo").str.extract_all(r"(\d+)").alias("extracted_nrs"))
print(out)

df = pl.DataFrame({"id": [1, 2], "text": ["123abc", "abc456"]})
out = df.with_columns(
    pl.col("text").str.replace(r"abc\b", "ABC"),
    pl.col("text").str.replace_all("a", "-", literal=True).alias("text_replace_all"),
)
print(out)
