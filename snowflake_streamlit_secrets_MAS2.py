import streamlit as st
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt
import numpy as np
import re

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

# Load data from Snowflake
def load_table_data(query):
    conn = create_snowflake_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()

# UI starts here
st.title("Hotel Insights Dashboard")

# Search hotels
search_term = st.text_input("Search Hotels by Name:")
if search_term:
    hotels = load_table_data(f"""
        SELECT PRODUCT_ID, HOTEL_NAME, CITY, STAR_RATING
        FROM PRODUCT_LIST
        WHERE HOTEL_NAME ILIKE '%{search_term}%'
    """)

    if hotels.empty:
        st.warning("No hotels found for the search term.")
    else:
        hotels["PRODUCT_ID"] = hotels["PRODUCT_ID"].astype(str)
        st.dataframe(hotels)
        selected_hotel = st.selectbox("Select a Hotel:", hotels["HOTEL_NAME"].tolist())

        if selected_hotel:
            selected_product_id = hotels.loc[hotels["HOTEL_NAME"] == selected_hotel, "PRODUCT_ID"].iloc[0]

            # Display product insights
            insights = load_table_data(f"""
                SELECT AMENITIES_SCORE, LOCATION_SCORE, DINING_SCORE, GENERAL_SCORE,
                    CLEANLINESS_SCORE, STAFF_SCORE, VALUE_FOR_MONEY_SCORE, ROOM_SCORE,
                    PRODUCT_SUMMARY, TOP_EMOTION_1, TOP_EMOTION_2, TOP_EMOTION_3, OVERALL_SCORE
                FROM PRODUCT_INSIGHT
                WHERE PRODUCT_ID = {selected_product_id}
            """)

            if not insights.empty:
                st.subheader("Product Insights")
                st.write(f"**Overall Score:** {round(insights['OVERALL_SCORE'].iloc[0])}")
                st.write(f"**Summary:** {insights['PRODUCT_SUMMARY'].iloc[0]}")

                aspects = load_table_data("SELECT DISTINCT ASPECT_NAME FROM ASPECT_LIST WHERE ASPECT_NAME != 'General'")
                selected_aspect = st.selectbox("Select an Aspect:", aspects["ASPECT_NAME"].tolist())

                if selected_aspect:
                    positive_button = st.button("Show Positive Mentions")
                    negative_button = st.button("Show Negative Mentions")

                    # Run separate SQL queries based on the button pressed
                    if positive_button:
                        reviews = load_table_data(f"""
                            SELECT SENTIMENT_TYPE, SENTIMENT_TEXT, START_INDEX, END_INDEX, CONFIDENCE_SCORE, REVIEW_TEXT
                            FROM PRODUCT_REVIEW_SNIPPET
                            WHERE PRODUCT_ID = {selected_product_id}
                            AND ASPECT_NAME = '{selected_aspect}'
                            AND SENTIMENT_TYPE = 'positive'
                            AND CONFIDENCE_SCORE > 0.8
                            ORDER BY CONFIDENCE_SCORE DESC
                        """)
                    elif negative_button:
                        reviews = load_table_data(f"""
                            SELECT SENTIMENT_TYPE, SENTIMENT_TEXT, START_INDEX, END_INDEX, CONFIDENCE_SCORE, REVIEW_TEXT
                            FROM PRODUCT_REVIEW_SNIPPET
                            WHERE PRODUCT_ID = {selected_product_id}
                            AND ASPECT_NAME = '{selected_aspect}'
                            AND SENTIMENT_TYPE = 'negative'
                            AND CONFIDENCE_SCORE > 0.8
                            ORDER BY CONFIDENCE_SCORE DESC
                        """)
                    else:
                        reviews = load_table_data(f"""
                            SELECT SENTIMENT_TYPE, SENTIMENT_TEXT, START_INDEX, END_INDEX, CONFIDENCE_SCORE, REVIEW_TEXT
                            FROM PRODUCT_REVIEW_SNIPPET
                            WHERE PRODUCT_ID = {selected_product_id}
                            AND ASPECT_NAME = '{selected_aspect}'
                            AND CONFIDENCE_SCORE > 0.8
                            ORDER BY CONFIDENCE_SCORE DESC
                        """)

                    # Pagination setup
                    reviews_per_page = st.selectbox("Reviews per page:", options=[10, 25], index=1)  # Default to 25
                    total_reviews = len(reviews)
                    max_page = int(np.ceil(total_reviews / reviews_per_page))
                    page = st.number_input("Select Page:", min_value=1, max_value=max_page, step=1)

                    start_idx = (page - 1) * reviews_per_page
                    end_idx = start_idx + reviews_per_page

                    # Display reviews
                    for idx, review in reviews.iloc[start_idx:end_idx].iterrows():
                        sentiment_color = "#90EE90" if review['SENTIMENT_TYPE'] == 'positive' else "#8B0000"
                        text_color = "black" if review['SENTIMENT_TYPE'] == 'positive' else "white"

                        # Highlight sentiment in the review text
                        highlighted_text = (
                            review['REVIEW_TEXT'][:review['START_INDEX']] +
                            f"<span style='background-color:{sentiment_color};font-weight:bold; color:{text_color};'>{review['SENTIMENT_TEXT']}</span>" +
                            review['REVIEW_TEXT'][review['END_INDEX']:]
                        )

                        st.markdown(highlighted_text, unsafe_allow_html=True)
                        st.divider()

                    if total_reviews == 0:
                        st.warning("No reviews to display.")
            else:
                st.warning("No insights found for the selected product.")
