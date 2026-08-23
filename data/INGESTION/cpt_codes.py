#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession, functions as f

# In[11]:


spark = SparkSession.builder.appName("CPT_CODE_LOADER").getOrCreate()

GCS_BUCKET = "healthcare-bucket-8826"
CPT_CODES_PATH = f"gs://healthcare-bucket-8826/landing/cptcodes/*.csv"
BQ_TABLE= "project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.cpt_codes"
TEMP_GCS_BUCKET = f"{GCS_BUCKET}/temp/"

#read the file from gcs

cpt_df = spark.read.csv(CPT_CODES_PATH, header=True)

for col in cpt_df.columns:
    new_col = col.replace(" ","_").lower()
    cpt_df = cpt_df.withColumnRenamed(col,new_col)
    

(
    cpt_df.write.format("BigQuery")
    .option("table" , BQ_TABLE)
    .option("temporaryGcsBucket" , TEMP_GCS_BUCKET )
    .mode("overwrite")
    .save()

)

# In[ ]:



