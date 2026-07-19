-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "7e491b48-5978-4a73-bbe7-b98df9812e65",
-- META       "default_lakehouse_name": "Bronze_LH",
-- META       "default_lakehouse_workspace_id": "2773bec8-6438-4872-b2ee-d34f1a32b3a9",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "7e491b48-5978-4a73-bbe7-b98df9812e65"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE IF NOT EXISTS FABRICTRAINING_INGESTXCEL.PERSON_BUSINESSENTITYCONTACT (
-- MAGIC     BusinessEntityID INT NOT NULL,
-- MAGIC     PersonID          INT NOT NULL,
-- MAGIC     ContactTypeID     INT NOT NULL,
-- MAGIC     rowguid           STRING NOT NULL,
-- MAGIC     ModifiedDate      TIMESTAMP NOT NULL
-- MAGIC ) USING DELTA

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
