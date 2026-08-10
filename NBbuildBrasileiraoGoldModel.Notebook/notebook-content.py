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
from pyspark.sql.window import Window

dfHistorical = spark.table(
    "lakehouseBrasileirao.dbo.silverbrasileiraohistorical"
)

display(dfHistorical)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfdimTeams = (
    dfHistorical
    .select("teamName")
    .distinct()
    .orderBy("teamName")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = Window.orderBy("teamName")

dfdimTeams = (
    dfdimTeams
    .withColumn(
        "teamKey",
        F.row_number().over(window_spec)
    )
    .select(
        "teamKey",
        "teamName"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dfdimTeams)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    dfdimTeams.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("lakehouseBrasileirao.dbo.goldDimTeams")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfHistorical = spark.table(
    "lakehouseBrasileirao.dbo.silverbrasileiraohistorical"
)

dfdimTeams = spark.table(
    "lakehouseBrasileirao.dbo.golddimteams"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_historical = (
    dfHistorical
    .join(
        dfdimTeams,
        on="teamName",
        how="left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_gold_historical.select(
        "year",
        "position",
        "teamName",
        "teamKey",
        "points"
    ).orderBy("year", "position")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_historical = df_gold_historical.select(
    "year",
    "position",
    "teamKey",
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

(
    df_gold_historical.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "lakehouseBrasileirao.dbo.goldbrasileiraohistorical"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_gold_historical)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
