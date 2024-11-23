# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cf7fe656-7ef3-4a3a-95d5-65a4f848e034",
# META       "default_lakehouse_name": "Sales_LH_Silver",
# META       "default_lakehouse_workspace_id": "9e7bf7af-e042-4baa-8e61-8dbb2a717c9f",
# META       "known_lakehouses": [
# META         {
# META           "id": "cf7fe656-7ef3-4a3a-95d5-65a4f848e034"
# META         },
# META         {
# META           "id": "f267edbc-42c2-4c08-87f5-f8f6380c9f4c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Sales Silver Flatten

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfBronze = spark.read.table("Sales_LH_Bronze.Sales_Bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSilver = dfBronze.dropDuplicates(["SalesOrderID", "SalesOrderDetailID"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import xpath, lit

dfSilver = dfSilver \
    .withColumn("TelephoneNumber", xpath(col("AdditionalContactInfo"), lit("//*[local-name()='telephoneNumber']/*[local-name()='number']/text()")).getItem(0)) \
    .withColumn("MobileNumber", xpath(col("AdditionalContactInfo"), lit("//*[local-name()='mobile']/*[local-name()='number']/text()")).getItem(0))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSilver = dfSilver \
    .withColumn("OrderDate", to_date("OrderDate")) \
    .withColumn("DueDate", to_date("DueDate")) \
    .withColumn("ShipDate", to_date("ShipDate")) \
    .withColumn("CurrencyRateDate", to_date("CurrencyRateDate"))

display(dfSilver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
