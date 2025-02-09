import streamlit as st
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt
import numpy as np

# Title
st.title("📊 Data Analysis: Reddit and Ecom")

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

# Tab selection
tab = st.tabs(["Reddit", "Ecom"])

# Reddit Tab
with tab[0]:
    st.header("📊 Reddit Analysis: iOS ↔ Android Switching Trends")

    ### 1️⃣ Quarterly Trends
    st.markdown("### 📅 Quarterly Trends in Platform Switching")
    st.divider()

    # Load data
    query_quarterly = "SELECT * FROM REDDIT.PUBLIC.QUARTERLY_TRENDS ORDER BY QUARTER"
    df_quarterly = load_table_data(query_quarterly)

    # Plot
    fig, ax = plt.subplots(figsize=(12, 6), dpi=100)
    width = 0.45
    x = np.arange(len(df_quarterly["QUARTER"]))

    ax.bar(x - width/2, df_quarterly["ANDROID_TO_IOS"], width, label="Android to iOS", color="blue", alpha=0.7)
    ax.bar(x + width/2, df_quarterly["IOS_TO_ANDROID"], width, label="iOS to Android", color="red", alpha=0.7)

    ax.set_xticks(x)
    ax.set_xticklabels(df_quarterly["QUARTER"], rotation=30, ha="right")
    ax.set_xlabel("Quarter", fontsize=12)
    ax.set_ylabel("Users", fontsize=12)
    ax.legend()
    st.pyplot(fig)
    st.divider()

    ### Additional Reddit sections (Reasons, Sentiment Analysis, etc.) follow the same logic as provided earlier.

# Ecom Tab
with tab[1]:
    st.header("📊 Ecom Analysis: Switching Trends and Insights")

    ### 1️⃣ Switch Source Count
    st.markdown("### 🔄 Switch Source Count (Amazon vs Flipkart)")
    st.divider()

    # Load data
    query_switch_source = "SELECT * FROM ECOM.PUBLIC.SWITCH_SOURCE"
    df_switch_source = load_table_data(query_switch_source)

    # Plot
    fig, ax = plt.subplots(figsize=(8, 5), dpi=100)
    x = np.arange(len(df_switch_source["SWITCH_DIRECTION"]))
    width = 0.35

    ax.bar(x - width/2, df_switch_source["AMAZON"], width, label="Amazon", color="blue", alpha=0.7)
    ax.bar(x + width/2, df_switch_source["FLIPKART"], width, label="Flipkart", color="orange", alpha=0.7)

    ax.set_xticks(x)
    ax.set_xticklabels(df_switch_source["SWITCH_DIRECTION"])
    ax.set_xlabel("Switch Direction", fontsize=12)
    ax.set_ylabel("Count", fontsize=12)
    ax.legend()
    st.pyplot(fig)
    st.divider()

    ### 2️⃣ Yearly Trends
    st.markdown("### 📅 Yearly Trends in Platform Switching")
    st.divider()

    query_yearly_trends = "SELECT * FROM ECOM.PUBLIC.YEARLY_TRENDS ORDER BY YEAR"
    df_yearly_trends = load_table_data(query_yearly_trends)

    fig, ax = plt.subplots(figsize=(12, 6), dpi=100)
    x = np.arange(len(df_yearly_trends["YEAR"]))

    ax.bar(x - width/2, df_yearly_trends["ANDROID_TO_IOS"], width, label="Android to iOS", color="blue", alpha=0.7)
    ax.bar(x + width/2, df_yearly_trends["IOS_TO_ANDROID"], width, label="iOS to Android", color="red", alpha=0.7)

    ax.set_xticks(x)
    ax.set_xticklabels(df_yearly_trends["YEAR"])
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Users", fontsize=12)
    ax.legend()
    st.pyplot(fig)
    st.divider()

    ### 3️⃣ Brand Origin
    st.markdown("### 🌍 Brand Origin Switch Count (iOS to Android Only)")
    st.divider()

    query_brand_origin = "SELECT * FROM ECOM.PUBLIC.BRAND_ORIGIN"
    df_brand_origin = load_table_data(query_brand_origin)

    fig, ax = plt.subplots(figsize=(8, 5), dpi=100)
    ax.bar(df_brand_origin["BRAND_ORIGIN"], df_brand_origin["SWITCH_COUNT"], color="green", alpha=0.7)

    ax.set_xlabel("Brand Origin", fontsize=12)
    ax.set_ylabel("Switch Count", fontsize=12)
    st.pyplot(fig)
    st.divider()

    ### 4️⃣ Sentiment Analysis
    st.markdown("### 😊 Sentiment Analysis of Switching Users")
    st.divider()

    query_sentiment = "SELECT * FROM ECOM.PUBLIC.SWITCH_SENTIMENT_SUMMARY"
    df_sentiment = load_table_data(query_sentiment)

    fig, ax = plt.subplots(figsize=(8, 5), dpi=100)
    ax.bar(df_sentiment["SWITCH_DIRECTION"], df_sentiment["POSITIVE"], label="Positive", color="green", alpha=0.7)
    ax.bar(df_sentiment["SWITCH_DIRECTION"], df_sentiment["NEGATIVE"], bottom=df_sentiment["POSITIVE"], label="Negative", color="red", alpha=0.7)

    ax.set_xlabel("Switch Direction", fontsize=12)
    ax.set_ylabel("Sentiment Count", fontsize=12)
    ax.legend()
    st.pyplot(fig)
    st.divider()

    ### 5️⃣ Switching Reasons
    st.markdown("### 🔍 Reasons for Switching")
    st.divider()

    query_reasons = "SELECT * FROM ECOM.PUBLIC.SWITCH_REASON"
    df_reasons = load_table_data(query_reasons)

    fig, ax = plt.subplots(figsize=(12, 6), dpi=100)
    x = np.arange(len(df_reasons["SWITCH_DIRECTION"]))

    ax.bar(x, df_reasons["AFFORDABILITY"], label="Affordability", color="blue", alpha=0.7)
    ax.bar(x, df_reasons["PERFORMANCE"], label="Performance", color="orange", alpha=0.7)

    ax.set_xticks(x)
    ax.set_xticklabels(df_reasons["SWITCH_DIRECTION"])
    ax.set_xlabel("Switch Direction", fontsize=12)
    ax.legend()
    st.pyplot(fig)
    st.divider()

    ### 6️⃣ Overall Summary
    st.markdown("### 📜 Overall Summary")
    st.divider()

    query_summary = "SELECT * FROM ECOM.PUBLIC.OVERALL_SUMMARY"
    df_summary = load_table_data(query_summary)

    # Convert DataFrame to Markdown-friendly format
    summary_table = "<table style='width:100%; border-collapse: collapse;'>"
    summary_table += "<tr><th>Title</th><th>Text</th></tr>"

    for _, row in df_summary.iterrows():
        summary_table += f"<tr><td>{row['SUMMARY_TITLE']}</td><td>{row['SUMMARY_TEXT']}</td></tr>"

    summary_table += "</table>"
    st.markdown(summary_table, unsafe_allow_html=True)

    st.success("✅ Ecom Analysis Completed!")
