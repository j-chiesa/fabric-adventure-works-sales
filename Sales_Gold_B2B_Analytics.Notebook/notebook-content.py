# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "145fe4f0-6424-4dbc-ad54-2a446a94a305",
# META       "default_lakehouse_name": "Sales_LH_Gold_B2C",
# META       "default_lakehouse_workspace_id": "9e7bf7af-e042-4baa-8e61-8dbb2a717c9f",
# META       "known_lakehouses": [
# META         {
# META           "id": "cf7fe656-7ef3-4a3a-95d5-65a4f848e034"
# META         },
# META         {
# META           "id": "145fe4f0-6424-4dbc-ad54-2a446a94a305"
# META         },
# META         {
# META           "id": "4647b1db-a9b7-4312-b2bd-23bc0b113514"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Sales Gold B2B Analytics

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

dfSilver = spark.read.table("Sales_LH_Silver.Sales_Silver_B2B")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dim Sales Order Detail

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales_LH_Gold_B2B.Dim_SalesOrderDetail") \
    .addColumn("SalesOrderID", IntegerType()) \
    .addColumn("SalesOrderDetailID", IntegerType()) \
    .addColumn("OrderQty", IntegerType()) \
    .addColumn("UnitPrice", DoubleType()) \
    .addColumn("UnitPriceDiscount", DoubleType()) \
    .addColumn("LineTotal", DoubleType()) \
    .execute()

dfDimSalesOrderDetail = dfSilver.dropDuplicates(["SalesOrderID", "SalesOrderDetailID"]).select(
    col("SalesOrderID"),
    col("SalesOrderDetailID"),
    col("OrderQty"),
    col("UnitPrice"),
    col("UnitPriceDiscount"),
    col("LineTotal")
)

deltaTable = DeltaTable.forName(spark, "Sales_LH_Gold_B2B.Dim_SalesOrderDetail")

dfUpdates = dfDimSalesOrderDetail

deltaTable.alias("gold") \
    .merge(
        dfUpdates.alias("updates"),
        "gold.SalesOrderID = updates.SalesOrderID AND gold.SalesOrderDetailID = updates.SalesOrderDetailID"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dim Status

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales_LH_Gold_B2B.Dim_Status") \
    .addColumn("StatusID", IntegerType()) \
    .addColumn("StatusDescription", StringType()) \
    .execute()

dfDimStatus = dfSilver.dropDuplicates(["Status"]).select(
    col("Status").alias("StatusID"),
    col("StatusDescription")
)

deltaTable = DeltaTable.forName(spark, "Sales_LH_Gold_B2B.Dim_Status")

dfUpdates = dfDimStatus

deltaTable.alias("gold") \
    .merge(
        dfUpdates.alias("updates"),
        "gold.StatusID = updates.StatusID"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dim Product

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales_LH_Gold_B2B.Dim_Product") \
    .addColumn("ProductID", IntegerType()) \
    .addColumn("ProductName", StringType()) \
    .addColumn("ProductNumber", StringType()) \
    .addColumn("Color", StringType()) \
    .addColumn("Size", StringType()) \
    .addColumn("Weight", DoubleType()) \
    .addColumn("SizeUnitMeasureCode", StringType()) \
    .addColumn("WeightUnitMeasureCode", StringType()) \
    .addColumn("ListPrice", DoubleType()) \
    .execute()

dfDimProduct = dfSilver.dropDuplicates(["ProductID"]).select(
    col("ProductID"),
    col("ProductName"),
    col("ProductNumber"),
    col("Color"),
    col("Size"),
    col("Weight"),
    col("SizeUnitMeasureCode"),
    col("WeightUnitMeasureCode"),
    col("ListPrice")
)

deltaTable = DeltaTable.forName(spark, "Sales_LH_Gold_B2B.Dim_Product")

dfUpdates = dfDimProduct

deltaTable.alias("gold") \
    .merge(
        dfUpdates.alias("updates"),
        "gold.ProductID = updates.ProductID"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dim Customer

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales_LH_Gold_B2B.Dim_Customer") \
    .addColumn("CustomerID", IntegerType()) \
    .addColumn("AccountNumber", StringType()) \
    .addColumn("StoreName", StringType()) \
    .addColumn("PersonTypeDescription", StringType()) \
    .addColumn("Title", StringType()) \
    .addColumn("FirstName", StringType()) \
    .addColumn("MiddleName", StringType()) \
    .addColumn("LastName", StringType()) \
    .addColumn("Suffix", StringType()) \
    .addColumn("TotalPurchaseYTD", DoubleType()) \
    .addColumn("EmailPromotionDescription", StringType()) \
    .addColumn("TelephoneNumber", StringType()) \
    .addColumn("MobileNumber", StringType()) \
    .execute()

dfDimCustomer = dfSilver.dropDuplicates(["CustomerID"]).select(
    col("CustomerID"),
    col("AccountNumber"),
    col("StoreName"),
    col("PersonTypeDescription"),
    col("Title"),
    col("FirstName"),
    col("MiddleName"),
    col("LastName"),
    col("Suffix"),
    col("TotalPurchaseYTD"),
    col("EmailPromotionDescription"),
    col("TelephoneNumber"),
    col("MobileNumber"),
)

deltaTable = DeltaTable.forName(spark, "Sales_LH_Gold_B2B.Dim_Customer")

dfUpdates = dfDimCustomer

deltaTable.alias("gold") \
    .merge(
        dfUpdates.alias("updates"),
        "gold.CustomerID = updates.CustomerID"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Dim Territory

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales_LH_Gold_B2B.Dim_Territory") \
    .addColumn("TerritoryID", IntegerType()) \
    .addColumn("CountryRegionCode", StringType()) \
    .addColumn("Group", StringType()) \
    .addColumn("LocalCurrency", StringType()) \
    .execute()

dfDimTerritory = dfSilver.dropDuplicates(["TerritoryID"]).select(
    col("TerritoryID"),
    col("CountryRegionCode"),
    col("Group"),
    col("LocalCurrency")
)

deltaTable = DeltaTable.forName(spark, "Sales_LH_Gold_B2B.Dim_Territory")

dfUpdates = dfDimTerritory

deltaTable.alias("gold") \
    .merge(
        dfUpdates.alias("updates"),
        "gold.TerritoryID = updates.TerritoryID"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Dim Date

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales_LH_Gold_B2B.Dim_Date") \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Day", IntegerType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn("Year", IntegerType()) \
    .addColumn("mmmyyyy", StringType()) \
    .addColumn("yyyymm", StringType()) \
    .execute()

dfDimDate = dfSilver.dropDuplicates(["OrderDate"]).select(
    col("OrderDate"), \
    dayofmonth("OrderDate").alias("Day"), \
    month("OrderDate").alias("Month"), \
    year("OrderDate").alias("Year"), \
    date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"), \
    date_format(col("OrderDate"), "yyyyMM").alias("yyyymm"), \
).orderBy("OrderDate")

deltaTable = DeltaTable.forName(spark, "Sales_LH_Gold_B2B.Dim_Date")

dfUpdates = dfDimDate

deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        'gold.OrderDate = updates.OrderDate'
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Fact Sales B2B

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales_LH_Gold_B2B.Fact_SalesB2B") \
    .addColumn("SalesOrderID", IntegerType()) \
    .addColumn("SalesOrderDetailID", IntegerType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("StatusID", IntegerType()) \
    .addColumn("ProductID", IntegerType()) \
    .addColumn("CustomerID", IntegerType()) \
    .addColumn("TerritoryID", IntegerType()) \
    .addColumn("SubTotal", DoubleType()) \
    .addColumn("TaxAmt", DoubleType()) \
    .addColumn("Freight", DoubleType()) \
    .addColumn("TotalDue", DoubleType()) \
    .addColumn("TotalDueLocalCurrency", DoubleType()) \
    .execute()

dfFactSalesB2B = dfSilver.dropDuplicates(["SalesOrderID", "SalesOrderDetailID"]).select(
    col("SalesOrderID"),
    col("SalesOrderDetailID"),
    col("OrderDate"),
    col("Status").alias("StatusID"),
    col("ProductID"),
    col("CustomerID"),
    col("TerritoryID"),
    col("SubTotal"),
    col("TaxAmt"),
    col("Freight"),
    col("TotalDue"),
    col("TotalDueLocalCurrency")
)

deltaTable = DeltaTable.forName(spark, "Sales_LH_Gold_B2B.Fact_SalesB2B")

dfUpdates = dfFactSalesB2B

deltaTable.alias("gold") \
    .merge(
        dfUpdates.alias("updates"),
        "gold.SalesOrderID = updates.SalesOrderID AND gold.SalesOrderDetailID = updates.SalesOrderDetailID"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
