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
# META       "default_lakehouse_workspace_id": "9e7bf7af-e042-4baa-8e61-8dbb2a717c9f"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Sales Silver Enrichment

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

dfSilver = spark.read.table("Sales_LH_Silver.Sales_Silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSilver = dfSilver.withColumn(
    "StatusDescription", 
    when(col("Status") == 1, "In process")
    .when(col("Status") == 2, "Approved")
    .when(col("Status") == 3, "Backordered")
    .when(col("Status") == 4, "Rejected")
    .when(col("Status") == 5, "Shipped")
    .when(col("Status") == 6, "Cancelled")
    .otherwise("Unknown")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSilver = dfSilver.fillna({
    "Color": "N/A",
    "Size": 0,
    "Weight": 0,
    "SizeUnitMeasureCode": "N/A",
    "WeightUnitMeasureCode": "N/A",
    "MiddleName": ""
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSilver = dfSilver.withColumn(
    "PersonType",
    when(col("PersonType") == "SC", 1)
    .when(col("PersonType") == "IN", 2)
    .when(col("PersonType") == "SP", 3)
    .when(col("PersonType") == "EM", 4)
    .when(col("PersonType") == "VC", 5)
    .when(col("PersonType") == "GC", 6)
    .otherwise(7)
)

dfSilver = dfSilver.withColumn(
    "PersonTypeDescription",
    when(col("PersonType") == 1, "Store contact")
    .when(col("PersonType") == 2, "Individual customer")
    .when(col("PersonType") == 3, "Sales person")
    .when(col("PersonType") == 4, "Employee")
    .when(col("PersonType") == 5, "Vendor contact")
    .when(col("PersonType") == 6, "General contact")
    .otherwise("Unknown")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSilver = dfSilver.withColumn(
    "EmailPromotionDescription",
    when(col("EmailPromotion") == 0, "Contact does not wish to receive e-mail promotions")
    .when(col("EmailPromotion") == 1, "Contact does wish to receive e-mail promotions from AdventureWorks")
    .when(col("EmailPromotion") == 2, "Contact does wish to receive e-mail promotions from AdventureWorks and selected partners")
    .otherwise("Unknown")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSilver = dfSilver.withColumn(
    "LocalCurrency",
    when(col("ToCurrencyCode").isNull(), "USD")
    .otherwise(col("ToCurrencyCode"))
)

dfSilver = dfSilver.withColumn(
    "TotalDueLocalCurrency",
    when(col("LocalCurrency") != "USD", col("TotalDue") * col("AverageRate"))
    .otherwise(col("TotalDue"))
)

dfSilver = dfSilver.withColumn(
    "LineTotalLocalCurrency",
    when(col("LocalCurrency") != "USD", col("LineTotal") * col("AverageRate"))
    .otherwise(col("LineTotal"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSilver = dfSilver \
    .withColumn("SubTotal", round(col("SubTotal"), 2)) \
    .withColumn("TaxAmt", round(col("TaxAmt"), 2)) \
    .withColumn("Freight", round(col("Freight"), 2)) \
    .withColumn("TotalDue", round(col("TotalDue"), 2)) \
    .withColumn("UnitPrice", round(col("UnitPrice"), 2)) \
    .withColumn("UnitPriceDiscount", round(col("UnitPriceDiscount"), 2)) \
    .withColumn("LineTotal", round(col("LineTotal"), 2)) \
    .withColumn("ListPrice", round(col("ListPrice"), 2)) \
    .withColumn("TotalDueLocalCurrency", round(col("TotalDueLocalCurrency"), 2)) \
    .withColumn("LineTotalLocalCurrency", round(col("LineTotalLocalCurrency"), 2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns_to_drop = [
    "RevisionNumber", 
    "Comment", 
    "MakeFlag",
    "FinishedGoodsFlag",
    "SellStartDate",
    "SellEndDate", 
    "DiscontinuedDate", 
    "CurrencyRateID", 
    "CurrencyRateDate", 
    "FromCurrencyCode", 
    "ToCurrencyCode", 
    "AverageRate", 
    "EndOfDateRate", 
    "AdditionalContactInfo", 
    "Demographics"
] 

dfSilver = dfSilver.drop(*columns_to_drop)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales Silver B2C

# CELL ********************

dfSilver_B2C = dfSilver.filter(col("PersonType") == 2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSilver = dfSilver \
    .withColumn("DateFirstPurchase", to_date(to_timestamp(col("DateFirstPurchase"), "yyyy-MM-ddX"))) \
    .withColumn("BirthDate", to_date(to_timestamp(col("BirthDate"), "yyyy-MM-ddX")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns_to_drop = [
    "OnlineOrderFlag", 
    "PurchaseOrderNumber", 
    "CarrierTrackingNumber", 
    "BusinessEntityID", 
    "Name", #Lo cambiare a StoreName
    "Title", 
    "NameStyle"
] 

dfSilver_B2C = dfSilver_B2C.drop(*columns_to_drop)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSilver.write.format("delta").mode("overwrite").save("Tables/Sales_Silver_B2C")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales Silver B2B
