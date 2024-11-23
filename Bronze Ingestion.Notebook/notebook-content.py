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

# # Bronze Ingestion

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

dfBronze = spark.read.format("parquet").load("Files/sales/AdventureWorksSales.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales_LH_Bronze.sales_bronze") \
    .addColumn("SalesOrderID", IntegerType()) \
    .addColumn("RevisionNumber", IntegerType()) \
    .addColumn("OrderDate", TimestampType()) \
    .addColumn("DueDate", TimestampType()) \
    .addColumn("ShipDate", TimestampType()) \
    .addColumn("Status", IntegerType()) \
    .addColumn("OnlineOrderFlag", BooleanType()) \
    .addColumn("SalesOrderNumber", StringType()) \
    .addColumn("PurchaseOrderNumber", StringType()) \
    .addColumn("CreditCardApprovalCode", StringType()) \
    .addColumn("SubTotal", DecimalType(19, 4)) \
    .addColumn("TaxAmt", DecimalType(19, 4)) \
    .addColumn("Freight", DecimalType(19, 4)) \
    .addColumn("TotalDue", DecimalType(19, 4)) \
    .addColumn("Comment", StringType()) \
    .addColumn("SalesOrderDetailID", IntegerType()) \
    .addColumn("CarrierTrackingNumber", StringType()) \
    .addColumn("OrderQty", IntegerType()) \
    .addColumn("UnitPrice", DecimalType(19, 4)) \
    .addColumn("UnitPriceDiscount", DecimalType(19, 4)) \
    .addColumn("LineTotal", DecimalType(38, 6)) \
    .addColumn("ProductID", IntegerType()) \
    .addColumn("ProductName", StringType()) \
    .addColumn("ProductNumber", StringType()) \
    .addColumn("MakeFlag", BooleanType()) \
    .addColumn("FinishedGoodsFlag", BooleanType()) \
    .addColumn("Color", StringType()) \
    .addColumn("Size", StringType()) \
    .addColumn("Weight", DecimalType(8, 2)) \
    .addColumn("SizeUnitMeasureCode", StringType()) \
    .addColumn("WeightUnitMeasureCode", StringType()) \
    .addColumn("SellStartDate", TimestampType()) \
    .addColumn("SellEndDate", TimestampType()) \
    .addColumn("DiscontinuedDate", TimestampType()) \
    .addColumn("ListPrice", DecimalType(19, 4)) \
    .addColumn("CurrencyRateID", IntegerType()) \
    .addColumn("CurrencyRateDate", TimestampType()) \
    .addColumn("FromCurrencyCode", StringType()) \
    .addColumn("ToCurrencyCode", StringType()) \
    .addColumn("AverageRate", DecimalType(19, 4)) \
    .addColumn("EndOfDayRate", DecimalType(19, 4)) \
    .addColumn("CreditCardID", IntegerType()) \
    .addColumn("CardType", StringType()) \
    .addColumn("CardNumber", StringType()) \
    .addColumn("ExpMonth", IntegerType()) \
    .addColumn("ExpYear", IntegerType()) \
    .addColumn("CustomerID", IntegerType()) \
    .addColumn("PersonID", IntegerType()) \
    .addColumn("AccountNumber", StringType()) \
    .addColumn("BusinessEntityID", IntegerType()) \
    .addColumn("Name", StringType()) \
    .addColumn("PersonType", StringType()) \
    .addColumn("Title", StringType()) \
    .addColumn("NameStyle", BooleanType()) \
    .addColumn("FirstName", StringType()) \
    .addColumn("MiddleName", StringType()) \
    .addColumn("LastName", StringType()) \
    .addColumn("EmailPromotion", IntegerType()) \
    .addColumn("Suffix", StringType()) \
    .addColumn("AdditionalContactInfo", StringType()) \
    .addColumn("TerritoryName", StringType()) \
    .addColumn("TerritoryID", IntegerType()) \
    .addColumn("CountryRegionCode", StringType()) \
    .addColumn("Group", StringType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltaTable = DeltaTable.forPath(spark, "Tables/sales_bronze")

deltaTable.alias("bronze")\
    .merge(
        dfBronze.alias("updates"),
        "bronze.SalesOrderID = updates.SalesOrderID AND bronze.SalesOrderDetailID = updates.SalesOrderDetailID"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
