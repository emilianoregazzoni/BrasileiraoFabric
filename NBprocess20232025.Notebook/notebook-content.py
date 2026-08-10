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

from pyspark.sql import functions as F

path = "Files/Bronze/api/standings/{2023,2024,2025}/standings.json"

df_raw = (
    spark.read
    .option("multiline", "true")
    .json(path)
)

display(df_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_standings = (
    df_raw
    .select(
        F.col("season.year").alias("year"),
        F.explode("standings").alias("standing")
    )
)

display(df_standings)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSilver20232025 = (
    df_standings
    .select(
        "year",
        F.col("standing.position").alias("position"),
        F.col("standing.team_name").alias("team"),
        F.col("standing.pts").alias("points"),
        F.col("standing.played").alias("games"),
        F.col("standing.won").alias("victories"),
        F.col("standing.drawn").alias("draws"),
        F.col("standing.lost").alias("losses"),
        F.col("standing.gf").alias("goals_scored"),
        F.col("standing.ga").alias("goals_against"),
        F.col("standing.gd").alias("goals_difference")
    )
)

display(dfSilver20232025)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Normalize special characters
import unicodedata
from pyspark.sql import functions as F

def remove_accents(text):
    if text is None:
        return None

    normalized = unicodedata.normalize("NFKD", text)

    return "".join(
        char for char in normalized
        if not unicodedata.combining(char)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

remove_accents_udf = F.udf(remove_accents, "string")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add new column like the silver historic from 2003-2022
dfSilver20232025 = (
    dfSilver20232025
    .withColumn(
        "teamName",
        remove_accents_udf(
            F.trim(F.col("team"))
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# check
display(
    dfSilver20232025.select(
        "team",
        "teamName"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# save DF as a new delta table in Lakehouse
(
    dfSilver20232025.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silverBrasileirao20232025")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
