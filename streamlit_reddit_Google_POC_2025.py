import streamlit as st
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt
import numpy as np

# Title
st.title("📊 Reddit Analysis: iOS ↔ Android Switching Trends")

# Snowflake connection function
def create_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

# Load data function
def load_table_data(query):
    conn = create_snowflake_connection()
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

### 1️⃣ Quarterly Trends
st.header("📅 Quarterly Trends in Platform Switching")

# Load data
query_quarterly = "SELECT * FROM REDDIT.PUBLIC.QUARTERLY_TRENDS ORDER BY QUARTER"
df_quarterly = load_table_data(query_quarterly)

# Plot
fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(df_quarterly["QUARTER"], df_quarterly["ANDROID_TO_IOS"], label="Android to iOS", color="blue", alpha=0.7)
ax.bar(df_quarterly["QUARTER"], df_quarterly["IOS_TO_ANDROID"], label="iOS to Android", color="red", alpha=0.7, bottom=df_quarterly["ANDROID_TO_IOS"])

ax.set_xlabel("Quarter")
ax.set_ylabel("Number of Users")
ax.set_title("Quarterly Trends in Switching")
ax.legend()
st.pyplot(fig)

# Divider
st.divider()

### 2️⃣ Reasons for Switching
st.header("🔄 Reasons for Switching")

# Load data
query_reasons = "SELECT * FROM REDDIT.PUBLIC.REASON_FOR_SWITCHING ORDER BY ANDROID_TO_IOS + IOS_TO_ANDROID DESC"
df_reasons = load_table_data(query_reasons)

# Bar Chart
fig, ax = plt.subplots(figsize=(10, 5))
bar_width = 0.4
x = np.arange(len(df_reasons["REASON"]))

ax.barh(x - bar_width/2, df_reasons["ANDROID_TO_IOS"], height=bar_width, label="Android to iOS", color="blue", alpha=0.7)
ax.barh(x + bar_width/2, df_reasons["IOS_TO_ANDROID"], height=bar_width, label="iOS to Android", color="red", alpha=0.7)

ax.set_yticks(x)
ax.set_yticklabels(df_reasons["REASON"])
ax.set_xlabel("Number of Users")
ax.set_title("Top Reasons for Switching")
ax.legend()
st.pyplot(fig)

# Divider
st.divider()

### 3️⃣ Sentiment Analysis
st.header("📊 Sentiment Analysis of Switching Users")

# Load data
query_sentiment = "SELECT * FROM REDDIT.PUBLIC.SENTIMENT_ANALYSIS"
df_sentiment = load_table_data(query_sentiment)

# Stacked Bar Chart
fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(df_sentiment["SWITCH_TYPE"], df_sentiment["POSITIVE"], label="Positive", color="green", alpha=0.7)
ax.bar(df_sentiment["SWITCH_TYPE"], df_sentiment["NEGATIVE"], bottom=df_sentiment["POSITIVE"], label="Negative", color="red", alpha=0.7)

ax.set_xlabel("Switch Type")
ax.set_ylabel("Sentiment Count")
ax.set_title("Sentiment Analysis of Platform Switching")
ax.legend()
st.pyplot(fig)

# Divider
st.divider()

### 4️⃣ Overall Summary
st.header("📜 Overall Summary")

# Load data
query_summary = "SELECT * FROM REDDIT.PUBLIC.SUMMARY"
df_summary = load_table_data(query_summary)

# Move "Overall" row to the bottom
df_summary_sorted = pd.concat([df_summary[df_summary["SUMMARY_TITLE"] != "Overall"], df_summary[df_summary["SUMMARY_TITLE"] == "Overall"]])

# Display table
st.dataframe(df_summary_sorted)

# End
st.success("✅ Analysis Completed!")
