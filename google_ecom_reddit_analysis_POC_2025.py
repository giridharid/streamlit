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

    ### 2️⃣ Reasons for Switching
    st.markdown("### 🔄 Reasons for Switching")
    st.divider()

    # Load data
    query_reasons = "SELECT * FROM REDDIT.PUBLIC.REASON_FOR_SWITCHING ORDER BY ANDROID_TO_IOS + IOS_TO_ANDROID DESC"
    df_reasons = load_table_data(query_reasons)

    # Dynamically adjust figure height based on number of rows
    fig_height = max(6, len(df_reasons) * 0.4)  # Scales height dynamically
    fig, ax = plt.subplots(figsize=(12, fig_height), dpi=100)  # Increased height

    bar_width = 0.4
    x = np.arange(len(df_reasons["REASON"]))

    bars1 = ax.barh(x - bar_width/2, df_reasons["ANDROID_TO_IOS"], height=bar_width, label="Android to iOS", color="blue", alpha=0.7)
    bars2 = ax.barh(x + bar_width/2, df_reasons["IOS_TO_ANDROID"], height=bar_width, label="iOS to Android", color="red", alpha=0.7)

    ax.set_yticks(x)
    ax.set_yticklabels(df_reasons["REASON"], fontsize=12)  # Increased font size
    ax.set_xlabel("Users", fontsize=12)
   # ax.set_title("Top Reasons for Switching", fontsize=14)
    ax.legend(loc="upper right", fontsize=12)  # Adjusted legend position

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
    st.divider()

    # Load data
    query_sentiment = "SELECT * FROM REDDIT.PUBLIC.SENTIMENT_ANALYSIS"
    df_sentiment = load_table_data(query_sentiment)

    # Adjust figure size & spacing
    fig, ax = plt.subplots(figsize=(8, 5), dpi=100)

    bars1 = ax.bar(df_sentiment["SWITCH_TYPE"], df_sentiment["POSITIVE"], label="Positive", color="green", alpha=0.7)
    bars2 = ax.bar(df_sentiment["SWITCH_TYPE"], df_sentiment["NEGATIVE"], bottom=df_sentiment["POSITIVE"], label="Negative", color="red", alpha=0.7)

    ax.set_xlabel("Switch Type", fontsize=12)
    ax.set_ylabel("Sentiment Count", fontsize=12)
    #ax.set_title("Sentiment Analysis", fontsize=14)

    # Adjust legend inside the bar area
    ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), fontsize=12, title="Sentiment")

    # Label values on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            y_offset = bar.get_y() if bars is bars2 else 0
            if height > 0:
                ax.annotate(f"{int(height)}", 
                            xy=(bar.get_x() + bar.get_width() / 2, height + y_offset),
                            xytext=(0, 3), textcoords="offset points", 
                            ha='center', fontsize=10)

    st.pyplot(fig)
    st.divider()

    ### 4️⃣ Overall Summary
    st.divider()


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

    st.divider()

    # End
    st.success("✅ Analysis Completed!")

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

  # Ecom Tab
#with tab[1]:
#    st.header("📊 Ecom Analysis: Switching Trends and Insights")

    ### 🔍 Reasons for Switching
    st.markdown("### 🔍 Reasons for Switching")
    st.divider()

    # Load the switch reasons data
    query_reasons = "SELECT * FROM ECOM.PUBLIC.SWITCH_REASON"
    df_switch_reason = pd.DataFrame({
        "switch_direction": ["Android to iOS", "iOS to Android"],
        "Affordability": [101, 124],
        "Apps": [25, 36],
        "Battery": [160, 193],
        "Camera": [158, 222],
        "Design or Experience": [119, 111],
        "Display": [119, 147],
        "Features": [44, 46],
        "Performance": [134, 175],
        "Other": [56, 82]  # Ignored later
    })

    # Define columns to include in the comparison (ignoring 'Other')
    columns_to_plot = [
        "Affordability", "Apps", "Battery", "Camera",
        "Design or Experience", "Display", "Features", "Performance"
    ]

    # Extract data for each switch direction
    x = np.arange(len(columns_to_plot))  # Positions for the bars
    width = 0.35  # Width of the bars

    android_to_ios = df_switch_reason.loc[df_switch_reason["switch_direction"] == "Android to iOS", columns_to_plot].iloc[0]
    ios_to_android = df_switch_reason.loc[df_switch_reason["switch_direction"] == "iOS to Android", columns_to_plot].iloc[0]

    # Set up the plot
    fig, ax = plt.subplots(figsize=(12, 6), dpi=100)

    # Create grouped bars
    bars_android_to_ios = ax.bar(x - width/2, android_to_ios, width, label="Android to iOS", color="blue", alpha=0.7)
    bars_ios_to_android = ax.bar(x + width/2, ios_to_android, width, label="iOS to Android", color="red", alpha=0.7)

    # Customize the chart
    ax.set_xticks(x)
    #ax.set_xticklabels(range(1, len(columns_to_plot) + 1))  # Replace labels with numbers
    ax.set_xticklabels(columns_to_plot, rotation=30, ha="right", fontsize=10)  # Use reasons as labels
    ax.set_ylabel("Count", fontsize=12)
    #ax.set_title("Comparison of Switching Reasons", fontsize=14)
    ax.legend(title="Switch Direction", loc="upper right")  # Legend for directions

    #ax.legend()

    # Annotate bars with their values
    for bars in [bars_android_to_ios, bars_ios_to_android]:
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.annotate(f"{int(height)}", 
                            xy=(bar.get_x() + bar.get_width() / 2, height), 
                            xytext=(0, 3), textcoords="offset points", 
                            ha='center', fontsize=9)

    # Display the plot in Streamlit
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
