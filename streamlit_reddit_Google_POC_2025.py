import streamlit as st
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt
import numpy as np

# Title
st.header("📊 Reddit Analysis: iOS ↔ Android Switching Trends")

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
st.markdown("### 📅 Quarterly Trends in Platform Switching")

# Load data
query_quarterly = "SELECT * FROM REDDIT.PUBLIC.QUARTERLY_TRENDS ORDER BY QUARTER"
df_quarterly = load_table_data(query_quarterly)

# Adjust figure size for better spacing
fig, ax = plt.subplots(figsize=(12, 6))  # Increased width and height
width = 0.35  # Adjusted bar width
x = np.arange(len(df_quarterly["QUARTER"]))

bars1 = ax.bar(x - width/2, df_quarterly["ANDROID_TO_IOS"], width, label="Android to iOS", color="blue", alpha=0.7)
bars2 = ax.bar(x + width/2, df_quarterly["IOS_TO_ANDROID"], width, label="iOS to Android", color="red", alpha=0.7)

ax.set_xticks(x)
ax.set_xticklabels(df_quarterly["QUARTER"], rotation=30, ha="right")  # Rotated for better readability
ax.set_xlabel("Quarter", fontsize=12)
ax.set_ylabel("Users", fontsize=12)
#ax.set_title("Quarterly Switching Trends", fontsize=14)
ax.legend()

# Label values on bars with better spacing
for bar in bars1 + bars2:
    height = bar.get_height()
    if height > 0:
        ax.annotate(f"{int(height)}", 
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 5), textcoords="offset points", 
                    ha='center', fontsize=10)

st.pyplot(fig)
st.divider()

st.divider()

### 2️⃣ Reasons for Switching
st.markdown("### 🔄 Reasons for Switching")

# Load data
query_reasons = "SELECT * FROM REDDIT.PUBLIC.REASON_FOR_SWITCHING ORDER BY ANDROID_TO_IOS + IOS_TO_ANDROID DESC"
df_reasons = load_table_data(query_reasons)

# Adjust figure size for better spacing
fig, ax = plt.subplots(figsize=(10, 6))  # Increased height
bar_width = 0.4
x = np.arange(len(df_reasons["REASON"]))

bars1 = ax.barh(x - bar_width/2, df_reasons["ANDROID_TO_IOS"], height=bar_width, label="Android to iOS", color="blue", alpha=0.7)
bars2 = ax.barh(x + bar_width/2, df_reasons["IOS_TO_ANDROID"], height=bar_width, label="iOS to Android", color="red", alpha=0.7)

ax.set_yticks(x)
ax.set_yticklabels(df_reasons["REASON"], fontsize=11)
ax.set_xlabel("Users", fontsize=12)
#ax.set_title("Top Reasons for Switching", fontsize=14)
ax.legend()

# Label values on bars with better spacing
for bars in [bars1, bars2]:
    for bar in bars:
        width = bar.get_width()
        if width > 0:
            ax.annotate(f"{int(width)}", 
                        xy=(width, bar.get_y() + bar.get_height() / 2),
                        xytext=(5, 0), textcoords="offset points", 
                        va='center', fontsize=10)

st.pyplot(fig)
st.divider()

### 3️⃣ Sentiment Analysis
st.markdown("### 📊 Sentiment Analysis of Switching Users")

# Load data
query_sentiment = "SELECT * FROM REDDIT.PUBLIC.SENTIMENT_ANALYSIS"
df_sentiment = load_table_data(query_sentiment)

# Stacked Bar Chart
fig, ax = plt.subplots(figsize=(6, 4))
bars1 = ax.bar(df_sentiment["SWITCH_TYPE"], df_sentiment["POSITIVE"], label="Positive", color="green", alpha=0.7)
bars2 = ax.bar(df_sentiment["SWITCH_TYPE"], df_sentiment["NEGATIVE"], bottom=df_sentiment["POSITIVE"], label="Negative", color="red", alpha=0.7)

ax.set_xlabel("Switch Type", fontsize=12)
ax.set_ylabel("Sentiment Count", fontsize=12)
#ax.set_title("Sentiment Analysis", fontsize=14)
ax.legend()

# Label values on bars
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        y_offset = bar.get_y() if bars is bars2 else 0
        if height > 0:
            ax.annotate(f"{int(height)}", xy=(bar.get_x() + bar.get_width() / 2, height + y_offset),
                        xytext=(0, 3), textcoords="offset points", ha='center', fontsize=10)

st.pyplot(fig)




# Divider
st.divider()

### 4️⃣ Overall Summary

# Load data
query_summary = "SELECT * FROM REDDIT.PUBLIC.SUMMARY"
df_summary = load_table_data(query_summary)

# Display summary with word wrapping
st.markdown("#### 📜 Overall Summary")

# Move "Overall" row to the bottom
df_summary_sorted = pd.concat([df_summary[df_summary["SUMMARY_TITLE"] != "Overall"], df_summary[df_summary["SUMMARY_TITLE"] == "Overall"]])

# Convert DataFrame to Markdown-friendly format
summary_table = "<table style='width:100%; border-collapse: collapse;'>"
summary_table += "<tr><th style='border: 1px solid black; padding: 8px; text-align: left;'>Title</th>" \
                 "<th style='border: 1px solid black; padding: 8px; text-align: left;'>Text</th></tr>"

for _, row in df_summary_sorted.iterrows():
    summary_table += f"<tr><td style='border: 1px solid black; padding: 8px; text-align: left;'>{row['SUMMARY_TITLE']}</td>" \
                     f"<td style='border: 1px solid black; padding: 8px; text-align: left; word-wrap: break-word; white-space: normal;'>{row['TEXT']}</td></tr>"

summary_table += "</table>"

# Display table using Markdown with HTML
st.markdown(summary_table, unsafe_allow_html=True)


# End
st.success("✅ Analysis Completed!")
