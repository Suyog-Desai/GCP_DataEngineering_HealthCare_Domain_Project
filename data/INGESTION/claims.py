#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession , functions as f

# In[10]:


spark = SparkSession.builder.appName("Healthcare_claim_injector").getOrCreate()

GCS_BUCKET = "healthcare-bucket-8826"
CLAIMS_PATH = f"gs://{GCS_BUCKET}/landing/claims/*.csv"
BQ_TABLE= "project-66ab2fa5-e082-4e6e-9c4.bronze_dataset.claims"
TEMP_GCS_BUCKET = f"{GCS_BUCKET}/temp/"

#read from claims
df = spark.read.csv(CLAIMS_PATH, header= True)
df = df.withColumn("datasource", 
                   f.when(f.input_file_name().contains("hospital2"), "hospb").
                   when(f.input_file_name().contains("hospital1"), "hospa").
                   otherwise("None"))
df = df.dropDuplicates()
                   
(
    df.write.format("bigquery")
    .option("table",BQ_TABLE)
    .option("temporaryGcsBucket",TEMP_GCS_BUCKET)
    .mode("overwrite")
    .save()
)


# In[ ]:



