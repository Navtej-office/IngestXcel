# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a6ee5274-a432-4395-b3a2-4f18886fce18",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "2773bec8-6438-4872-b2ee-d34f1a32b3a9",
# META       "known_lakehouses": [
# META         {
# META           "id": "a6ee5274-a432-4395-b3a2-4f18886fce18"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.product (
# MAGIC     ProductID INT, Name STRING, ProductNumber STRING, MakeFlag STRING,
# MAGIC     FinishedGoodsFlag STRING, Color STRING, SafetyStockLevel SMALLINT,
# MAGIC     ReorderPoint SMALLINT, StandardCost DECIMAL(19,4), ListPrice DECIMAL(19,4),
# MAGIC     Size STRING, SizeUnitMeasureCode STRING, WeightUnitMeasureCode STRING,
# MAGIC     Weight DECIMAL(8,2), DaysToManufacture INT, ProductLine STRING, Class STRING,
# MAGIC     Style STRING, ProductSubcategoryID INT, ProductModelID INT,
# MAGIC     SellStartDate TIMESTAMP, SellEndDate TIMESTAMP, DiscontinuedDate TIMESTAMP,
# MAGIC     rowguid STRING, ModifiedDate TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.productcategory (
# MAGIC     ProductCategoryID INT, Name STRING, rowguid STRING, ModifiedDate TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.currency (
# MAGIC     CurrencyCode STRING, Name STRING, ModifiedDate TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.persondetails (
# MAGIC     ID INT, Name STRING, Age INT, DateOfBirth DATE, Address STRING
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Create Schema fabrictraining_ingestxcel;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
