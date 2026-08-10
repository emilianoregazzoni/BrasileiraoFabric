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
import unicodedata

def remove_accents(text):
    if text is None:
        return None

    normalized = unicodedata.normalize("NFKD", text)

    return "".join(
        c for c in normalized
        if not unicodedata.combining(c)
    )

remove_accents_udf = F.udf(remove_accents, "string")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_standings = "Files/Bronze/api/standings/2026/standings.json"

df_standings_raw = (
    spark.read
    .option("multiline", "true")
    .json(path_standings)
)

display(df_standings_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_standings = (
    df_standings_raw
    .select(
        F.col("season.id").alias("season_id"),
        F.col("season.year").alias("year"),
        F.explode("standings").alias("standing")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_standings_silver = (
    df_standings
    .select(
        "season_id",
        "year",
        F.col("standing.position").alias("position"),
        F.col("standing.team_id").alias("team_id"),
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_standings_silver = (
    df_standings_silver
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

df_standings_silver = df_standings_silver.select(
    "season_id",
    "year",
    "position",
    "team_id",
    "team",
    "teamName",
    "points",
    "games",
    "victories",
    "draws",
    "losses",
    "goals_scored",
    "goals_against",
    "goals_difference"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_standings_silver.orderBy("position")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_standings_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "lakehouseBrasileirao.dbo.silverBrasileiraoCurrent2026"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# scorers
path_scorers = "Files/Bronze/api/standings/2026/scorers.json"

df_scorers_raw = (
    spark.read
    .option("multiline", "true")
    .json(path_scorers)
)

display(df_scorers_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_scorers = (
    df_scorers_raw
    .select(
        F.explode("leaders").alias("scorer")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_scorers_silver = (
    df_scorers
    .select(
        F.col("scorer.rank").alias("rank"),
        F.col("scorer.player_id").alias("player_id"),
        F.col("scorer.player_name").alias("player_name"),
        F.col("scorer.position").alias("player_position"),
        F.col("scorer.team_id").alias("team_id"),
        F.col("scorer.team_name").alias("team"),
        F.col("scorer.value").alias("goals"),
        F.col("scorer.matches").alias("matches")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_scorers_silver = (
    df_scorers_silver
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

df_scorers_silver = df_scorers_silver.select(
    "rank",
    "player_id",
    "player_name",
    "player_position",
    "team_id",
    "team",
    "teamName",
    "goals",
    "matches"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# check
display(
    df_scorers_silver.orderBy("rank")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_scorers_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "lakehouseBrasileirao.dbo.silverBrasileiraoScorers2026"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
