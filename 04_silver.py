class silver:
    
    @staticmethod
    def transformation():
        # Define the window specification for deduplication
        rowDedup = key(["example_email_column"])

        # Read the bronze table and perform transformations
        df = (
            spark.read.table("crm_bronze")
            .withColumn(
                # Reverse and format the CNPJ column
                "nr_cnpj_estb", F.reverse(F.substring(F.concat(F.reverse(F.col("nr_cnpj_estb")), F.lit("00000000000000")), 0, 14))
            )
            .withColumn(
                # Trim the agency code column
                "cd_agen_estb", F.trim(F.col("cd_agen_estb"))
            )
            .withColumn(
                # Trim the account number column
                "nr_cnta_crrt_estb", F.trim(F.col("nr_cnta_crrt_estb"))
            )
            .withColumn(
                # Standardize email format and filter invalid emails
                "nm_email_1", F.when(
                    (F.lower(F.col("nm_email_1")).contains("@")) &
                    (
                        (F.lower(F.col("nm_email_1")).like("%__@___%.com.__")) |
                        (F.lower(F.col("nm_email_1")).like("%.com.br"))
                    ),
                    F.lower(F.col("nm_email_1"))
                )
            )
            .withColumn(
                # Add a row number column for deduplication
                "rowDedup", F.row_number().over(rowDedup)
            )
            .select(
                # Select and alias columns
                F.col("nr_cnpj_estb").alias("cnpj"),
                F.col("nm_email_1").alias("email"),
                F.col("cd_agen_estb").alias("agencia"),
                F.col("nr_cnta_crrt_estb").alias("conta"),
                F.col("data_ref_carga"),
                F.col("ingest_time")
            )
            .filter(
                # Filter rows to keep only the first occurrence per group
                F.col("rowDedup") == 1
            )
        )

        # Read and deduplicate API data
        df_api = spark.read.table("bronze_api").dropDuplicates(["email"])
        # Read and deduplicate file data
        df_file = spark.read.table("bronze_file").dropDuplicates(["CPFCNPJ"])

        # Join the DataFrames
        df_matches = (
            df.alias("a")
            .join(df_api.alias("b"), df.email == df_api.email, "left")
            .join(df_file.alias("c"), F.expr("a.cnpj like concat('%', c.CPFCNPJ, '%')"), "left")
            .withColumn(
                # Mark matches from API
                "match_api", F.when(F.col("b.email").isNotNull(), 1).otherwise(0)
            )
            .withColumn(
                # Mark matches from file
                "match_file", F.when(F.col("c.CPFCNPJ").isNotNull(), 1).otherwise(0)
            )
            .select(
                "a.*",
                F.col("match_api"),
                F.col("match_file")
            )
        )

        # Observability: Print counts to verify the transformations
        print("Count (original): {}".format(df.count()))
        print("Count (transform): {}".format(df_matches.count()))

        if df.count() != df_matches.count():
            print("Join key duplication error.")
        
        # Create or replace a temporary view with the transformed DataFrame
        df_matches.createOrReplaceTempView("crm_silver")
