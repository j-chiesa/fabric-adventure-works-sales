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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Sales Gold B2C Analytics

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

dfSilver = spark.read.table("Sales_LH_Silver.Sales_Silver_B2C")
display(dfSilver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dim Sales Order Detail

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

DeltaTable.createIfNotExists(spark) \
.tableName("DimSalesOrderDetail") \
.addColumn("SalesOrderDetailID", IntegerType()) \
.addColumn("OrderQty", IntegerType()) \
.addColumn("UnitPrice", DoubleType()) \
.addColumn("UnitPriceDiscount", DoubleType()) \
.addColumn("LineTotal", DoubleType()) \
.execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales_LH_Gold_B2C.Dim.DimSalesOrderDetail") \
    .addColumn("SalesOrderDetailID", IntegerType()) \
    .addColumn("OrderQty", IntegerType()) \
    .addColumn("UnitPrice", DoubleType()) \
    .addColumn("UnitPriceDiscount", DoubleType()) \
    .addColumn("LineTotal", DoubleType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# Intentar acceder a la tabla Delta
try:
    deltaTable = DeltaTable.forName(spark, "Sales_LH_Gold_B2C.dbo.DimSalesOrderDetail")
except AnalysisException:
    # Crear un DataFrame vacío con el esquema deseado si la tabla no existe
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

    schema = StructType([
        StructField("SalesOrderDetailID", IntegerType()),
        StructField("OrderQty", IntegerType()),
        StructField("UnitPrice", DoubleType()),
        StructField("UnitPriceDiscount", DoubleType()),
        StructField("LineTotal", DoubleType())
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").saveAsTable("dbo.DimSalesOrderDetail")
    
    # Ahora, la tabla existe y podemos acceder a ella
    deltaTable = DeltaTable.forName(spark, "dbo.DimSalesOrderDetail")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
