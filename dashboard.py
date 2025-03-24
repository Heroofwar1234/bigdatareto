import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession

# Streamlit page configuration
st.set_page_config(page_title="People Data Dashboard", layout="wide")

# Sidebar setup
sidebar = st.sidebar
sidebar.title("People Data Filters")
sidebar.write("Use the filters below to refine the data.")

# Initialize Spark session
spark = SparkSession.builder.appName("PeopleDataDashboard").getOrCreate()

# Load dataset
df = spark.read.csv("people.csv", header=True, inferSchema=True)
df_pandas = df.toPandas()  # Convert to Pandas for Streamlit

# Sidebar filter - Gender
gender_options = ["All"] + df_pandas["sex"].unique().tolist()
gender_filter = sidebar.selectbox("Select Gender:", gender_options)

# Apply gender filter
if gender_filter != "All":
    df_pandas = df_pandas[df_pandas["sex"] == gender_filter]

# Main section
st.title("People Data Dashboard")
st.header("Dataset Overview")
st.write("Explore the dataset interactively.")

# Checkbox to show DataFrame
if sidebar.checkbox("Show Data"):
    st.subheader("Filtered Data")
    st.dataframe(df_pandas)

# Sample Data Visualization
st.header("Sample Data Visualization")
st.write("Below is a small sample from the dataset.")

sample_data = df_pandas.sample(n=10) if len(df_pandas) > 10 else df_pandas
st.dataframe(sample_data)

# Close Spark session
spark.stop()
