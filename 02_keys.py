def key(fieldlist):
    # Create a window specification partitioned by the given fields
    window = wd.partitionBy(
        *fieldlist
    ).orderBy(
        # Order the partitions by the specified columns in descending order
        F.desc(F.col("example_field_1")), F.desc(F.col("example_field_2"))
    )
    return window
