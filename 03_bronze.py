class bronze:
    
    @staticmethod
    def crm():
        # Read data from the specified table with a limit of 100 rows
        df = (spark.read
                    .table("example_database.example_table")
                    .limit(100)
                    # Filters
                    .filter(F.col("example_column") == "example_value")
                    # Select specific columns
                    .select(
                        ["example_col1", "example_col2", "example_col3", "example_col4"]
                    )
                    # Add a column with the current timestamp
                    .withColumn(
                        "ingest_time", F.current_timestamp()
                    )
        )
        # Create or replace a temporary view with the DataFrame
        df.createOrReplaceTempView("crm_bronze")

    @staticmethod
    def api():
        # Example JSON data
        json_raw = [{"email": "example1@example.com"}, {"email": "example2@example.com"}]
        # Create DataFrame from JSON data
        df = spark.createDataFrame(json_raw)
        # Create or replace a temporary view with the DataFrame
        df.createOrReplaceTempView("bronze_api")

    @staticmethod
    def file():
        # Read CSV file with specified options
        file = (spark.read.format("csv")
                .option("header", "true")
                .option("delimiter", ";")
                .load("/FileStore/EXAMPLE_FILE.csv"))
        
        # Example new rows to be added to the DataFrame
        json_raw = [
            {"example_col1": "example_value1", "example_col2": "example_value2", "example_col3": "example_value3"},
            {"example_col1": "example_value4", "example_col2": "example_value5", "example_col3": "example_value6"}
        ]
        # Create DataFrame from new rows
        newRows = spark.createDataFrame(json_raw)
        # Union the new rows with the original DataFrame
        df = file.union(newRows)
        # Create or replace a temporary view with the DataFrame
        df.createOrReplaceTempView("bronze_file")
