import streamlit as st
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt
import numpy as np
import re
from transformers import pipeline

# Load the punctuate model
punctuator = pipeline("text2text-generation", model="Vamsi/T5_Paraphrase_Punctuation")


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

# Function to punctuate, capitalize, and highlight text
def punctuate_and_highlight(review_text, sentiment_text):
    """
    Punctuates the review text, capitalizes sentences, and highlights the sentence encapsulating the sentiment_text.

    Args:
        review_text (str): The full review text.
        sentiment_text (str): The text to find within a sentence.

    Returns:
        str: The review_text with the relevant sentence highlighted in HTML.
    """
    punctuated_text = punctuator(review_text)[0]['generated_text']

    sentences = re.split(r'(?<=[.!?])\s+', punctuated_text)
    sentences = [sentence.capitalize() if not sentence.isupper() else sentence for sentence in sentences]

    for sentence in sentences:
        if sentiment_text.lower() in sentence.lower():
            highlighted_sentence = f"<span style='background-color:yellow;font-weight:bold;'>{sentence}</span>"
            return punctuated_text.replace(sentence, highlighted_sentence)

    return punctuated_text


# **Search Hotels**
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

            # **Aspect Selection**
            aspects = load_table_data("SELECT DISTINCT ASPECT_NAME FROM ASPECT_LIST WHERE ASPECT_NAME != 'General'")
            selected_aspect = st.selectbox("Select an Aspect:", aspects["ASPECT_NAME"].tolist())

            if selected_aspect:
                # **Language Selection**
                tab1, tab2, tab3 = st.tabs(["English", "Hindi", "Tamil"])
                
                # **Query the correct review table**
                reviews = load_table_data(f"""
                    SELECT SENTIMENT_TYPE, SENTIMENT_TEXT, SENTIMENT_TEXT_HI, SENTIMENT_TEXT_TA,
                           START_INDEX, END_INDEX, CONFIDENCE_SCORE, REVIEW_TEXT, REVIEW_TEXT_HI, REVIEW_TEXT_TA
                    FROM PRODUCT_MULTI_LANG_REVIEW_SNIPPET
                    WHERE PRODUCT_ID = {selected_product_id}
                    AND ASPECT_NAME = '{selected_aspect}'
                    AND CONFIDENCE_SCORE > 0.8
                    ORDER BY CONFIDENCE_SCORE DESC
                """)

                if not reviews.empty:
                    # **Review Pagination**
                    reviews_per_page = st.selectbox("Reviews per page:", options=[10, 25, 50], index=1)
                    total_reviews = len(reviews)
                    max_page = int(np.ceil(total_reviews / reviews_per_page))
                    page = st.number_input("Select Page:", min_value=1, max_value=max_page, step=1)

                    start_idx = (page - 1) * reviews_per_page
                    end_idx = start_idx + reviews_per_page

                    reviews_batch = reviews.iloc[start_idx:end_idx]

                    # **Display English Reviews**
                    with tab1:
                        st.subheader(f"Reviews in English ({selected_aspect})")
                        for _, review in reviews_batch.iterrows():
                            color = "#90EE90" if review['SENTIMENT_TYPE'] == 'positive' else "#8B0000"
                            text_color = "black" if review['SENTIMENT_TYPE'] == 'positive' else "white"

                            highlighted_text = punctuate_and_highlight(review['REVIEW_TEXT'], review['SENTIMENT_TEXT'])
                            st.markdown(f"<div style='background-color:{color}; color:{text_color}; padding:10px;'>{highlighted_text}</div>", unsafe_allow_html=True)

                    # **Display Hindi Reviews**
                    with tab2:
                        st.subheader(f"Reviews in Hindi ({selected_aspect})")
                        for _, review in reviews_batch.iterrows():
                            color = "#90EE90" if review['SENTIMENT_TYPE'] == 'positive' else "#8B0000"
                            text_color = "black" if review['SENTIMENT_TYPE'] == 'positive' else "white"

                            highlighted_text = punctuate_and_highlight(review['REVIEW_TEXT_HI'], review['SENTIMENT_TEXT_HI'])
                            st.markdown(f"<div style='background-color:{color}; color:{text_color}; padding:10px;'>{highlighted_text}</div>", unsafe_allow_html=True)

                    # **Display Tamil Reviews**
                    with tab3:
                        st.subheader(f"Reviews in Tamil ({selected_aspect})")
                        for _, review in reviews_batch.iterrows():
                            color = "#90EE90" if review['SENTIMENT_TYPE'] == 'positive' else "#8B0000"
                            text_color = "black" if review['SENTIMENT_TYPE'] == 'positive' else "white"

                            highlighted_text = punctuate_and_highlight(review['REVIEW_TEXT_TA'], review['SENTIMENT_TEXT_TA'])
                            st.markdown(f"<div style='background-color:{color}; color:{text_color}; padding:10px;'>{highlighted_text}</div>", unsafe_allow_html=True)
