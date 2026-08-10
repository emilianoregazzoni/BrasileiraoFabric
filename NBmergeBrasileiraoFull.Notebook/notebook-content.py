# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "490b5693-ab01-4afb-abc0-077f72fda460",
# META       "default_lakehouse_name": "lakehouseBrasileirao",
# META       "default_lakehouse_workspace_id": "151fcb69-dedf-4d5c-9f1f-0638abe5dfc1",
# META       "known_lakehouses": [
# META         {
# META           "id": "490b5693-ab01-4afb-abc0-077f72fda460"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.table("silverBrasileirao20032022").printSchema()
spark.table("silverBrasileirao20232025").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_old = (
    spark.table("silverBrasileirao20032022")
    .drop("total_points")
)

df_new = spark.table("silverBrasileirao20232025")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_historical = df_old.unionByName(df_new)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Filas:", df_historical.count())

display(
    df_historical
    .groupBy("year")
    .count()
    .orderBy("year")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_historical
    .orderBy("year", "position")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_historical = (
    df_historical
    .withColumn(
        "teamName",
        F.when(
            F.col("teamName").isin(
                "Atletico Paranaense",
                "Athletico Paranaenseb?",
                "Athletico"
            ),
            "Athletico Paranaense"
        )
        .when(
            F.col("teamName").isin(
                "Palmeirasa?",
                "Palmeiras"
            ),
            "Palmeiras"
        )
        .when(
            F.col("teamName").isin(
                "RB Bragantino",
                "Red Bull Bragantino"
            ),
            "Red Bull Bragantino"
        )
        .when(
            F.col("teamName").isin(
                "Sport",
                "Sport Recife"
            ),
            "Sport Recife"
        )
        .otherwise(F.col("teamName"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_historical
    .orderBy("year", "position")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_historical.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silverBrasileiraoHistorical")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
