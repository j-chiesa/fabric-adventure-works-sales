# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f267edbc-42c2-4c08-87f5-f8f6380c9f4c",
# META       "default_lakehouse_name": "Sales_LH_Bronze",
# META       "default_lakehouse_workspace_id": "9e7bf7af-e042-4baa-8e61-8dbb2a717c9f"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Sales Bronze Ingestion

# CELL ********************

from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("parquet").load("Files/sales/AdventureWorksSales.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    deltaTable = DeltaTable.forName(spark, "Sales_LH_Bronze.Sales_Bronze")
except AnalysisException:
    df.write.format("delta").mode("overwrite").saveAsTable("Sales_LH_Bronze.Sales_Bronze")
    deltaTable = DeltaTable.forName(spark, "Sales_LH_Bronze.Sales_Bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltaTable.alias("bronze") \
    .merge(
        df.alias("updates"),
        "bronze.SalesOrderID = updates.SalesOrderID AND bronze.SalesOrderDetailID = updates.SalesOrderDetailID"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
