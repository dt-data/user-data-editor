import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from delta.tables import DeltaTable
import streamlit as st

# Initialize Spark Session
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"  # Adjust if needed
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages io.delta:delta-core_2.12:2.4.0 pyspark-shell"

spark = SparkSession.builder \
    .appName("DeltaTableEditor") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define Delta Table Path
delta_table_path = "./delta-table"  # Relative path for Streamlit Cloud

# Create or Load Delta Table
if not os.path.exists(delta_table_path):
    # Sample Data
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    data = [(1, "Alice", 29), (2, "Bob", 31), (3, "Cathy", 24)]
    df = spark.createDataFrame(data, schema=schema)

    # Save as Delta Table
    df.write.format("delta").mode("overwrite").save(delta_table_path)
else:
    st.info("Delta Table exists, loading...")
    
# Streamlit App
st.title("Delta Table Editor")

# Read Delta Table
delta_df = spark.read.format("delta").load(delta_table_path)
pandas_df = delta_df.toPandas()

# Use Streamlit's data_editor
st.write("### Edit the Delta Table Below:")
edited_df = st.data_editor(pandas_df, use_container_width=True)

# Display the updated data
st.write("### Updated DataFrame:")
st.write(edited_df)

# Save Changes to Delta Table
if st.button("Save Changes"):
    # Convert back to Spark DataFrame
    updated_spark_df = spark.createDataFrame(edited_df)

    # Overwrite the Delta Table with updated data
    updated_spark_df.write.format("delta").mode("overwrite").save(delta_table_path)
    st.success("Delta Table updated successfully!")
