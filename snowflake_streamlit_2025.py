import streamlit as st
import pandas as pd
import snowflake.connector
import configparser
import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px
import re  # Regular expressions for extracting context
from sqlalchemy import create_engine

# Snowflake connection function
def create_snowflake_connection():
    config = configparser.ConfigParser()
    config.read("parameter.txt")

    user = config.get("Snowflake_connector", "USER")
    password = config.get("Snowflake_connector", "PASSWORD")
    account = config.get("Snowflake_connector", "ACCOUNT")
    warehouse = config.get("Snowflake_connector", "WAREHOUSE")
    database = config.get("Snowflake_connector", "DATABASE")
    schema = config.get("Snowflake_connector", "SCHEMA")

    # Creating Snowflake connection using SQLAlchemy
    connection_string = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"
    engine = create_engine(connection_string)

    return engine

# Load data from Snowflake using SQLAlchemy engine and convert column names to uppercase
def load_table_data(query):
    engine = create_snowflake_connection()
    df = pd.read_sql(query, engine)
    
    # Convert all column names to uppercase
    df.columns = df.columns.str.upper()
    
    return df

# Function to extract full sentence containing the sentiment text
def extract_sentence(review_text, sentiment_text):
    # Regular expression to extract full sentence containing the sentiment text
    pattern = r"([^.]*?{}[^.]*\.)".format(re.escape(sentiment_text))
    matches = re.findall(pattern, review_text)
    
    # If sentiment text is found, return the full sentence
    if matches:
        return matches[0]
    else:
        return review_text.split(".")[0]  # Return first sentence if no match

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
        # Convert PRODUCT_ID to string to avoid formatting issues
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
                st.write(f"**Overall Score:** {round(insights['OVERALL_SCORE'].iloc[0])}")  # Round to nearest integer
                st.write(f"**Summary:** {insights['PRODUCT_SUMMARY'].iloc[0]}")
                st.divider()
                # Conditionally display top emotions
                top_emotions = [insights.iloc[0]["TOP_EMOTION_1"], insights.iloc[0]["TOP_EMOTION_2"], insights.iloc[0]["TOP_EMOTION_3"]]
                top_emotions = [emotion for emotion in top_emotions if pd.notna(emotion)]

                if top_emotions:
                    st.subheader("Top Emotions")
                    st.write(", ".join(top_emotions))
                else:
                    st.write("No emotions available.")
                st.divider()
                # Display aspect scores dynamically with colorful bars
                st.subheader("Aspect Scores")

                # Filter columns with "_SCORE" in their name dynamically
                aspect_columns = [col for col in insights.columns if col.endswith("_SCORE")]
                aspect_columns = [col for col in aspect_columns if col != "GENERAL_SCORE"]

                aspect_names = [col.replace("_SCORE", "").replace("_", " ").capitalize() for col in aspect_columns]
                valid_scores = insights.iloc[0][aspect_columns].dropna()

                if len(valid_scores) == len(aspect_names):
                    aspect_scores = pd.DataFrame({
                        "Aspect": aspect_names,
                        "Score": valid_scores.values
                    })

                    fig, ax = plt.subplots()
                    bars = ax.bar(aspect_scores["Aspect"], aspect_scores["Score"], color=plt.cm.viridis(np.linspace(0, 1, len(aspect_scores))))
                    ax.set_xlabel('Aspect')
                    ax.set_ylabel('Score')
                    ax.set_title('Aspect Scores')

                    for bar in bars:
                        yval = round(bar.get_height())
                        ax.text(bar.get_x() + bar.get_width() / 2, yval + 0.1, yval, ha='center', va='bottom')

                    plt.xticks(rotation=45, ha='right')
                    st.pyplot(fig)
                else:
                    st.warning("Mismatch between aspect names and aspect scores.")
                st.divider()
                # Select aspect from Aspect List table
                aspects = load_table_data("SELECT DISTINCT ASPECT_NAME FROM ASPECT_LIST WHERE ASPECT_NAME != 'General'")
                selected_aspect = st.selectbox("Select an Aspect:", aspects["ASPECT_NAME"].tolist())

                if selected_aspect:
                    # Get top positive and negative phrases for the selected aspect
                    top_phrases = load_table_data(f"""
                        SELECT POSITIVE_PHRASES, NEGATIVE_PHRASES
                        FROM PRODUCT_ASPECT_TOP_PHRASE
                        WHERE PRODUCT_ID = {selected_product_id} AND ASPECT = '{selected_aspect}'
                    """)

                    if not top_phrases.empty:
                        st.subheader("Top Phrases")
                        positive_phrases = top_phrases['POSITIVE_PHRASES'].iloc[0]
                        negative_phrases = top_phrases['NEGATIVE_PHRASES'].iloc[0]

                    # Create two columns for displaying phrases side by side
                        col1, col2 = st.columns([1, 1])  # Equal width columns


                        # Display positive phrases in the first column
                        if positive_phrases:
                            positive_phrases = positive_phrases.split(",")
                            with col1:
                                st.write("**Positive Phrases**")
                                for phrase in positive_phrases:
                                    st.markdown(f"<div style='background-color:#d4edda;padding:10px;'><b>{phrase.strip()}</b></div>", unsafe_allow_html=True)
                        else:
                            with col1:
                                st.write("No positive phrases found.")

                        # Display negative phrases in the second column
                        if negative_phrases:
                            negative_phrases = negative_phrases.split(",")
                            with col2:
                                st.write("**Negative Phrases**")
                                for phrase in negative_phrases:
                                    st.markdown(f"<div style='background-color:#f8d7da;padding:10px;'><b>{phrase.strip()}</b></div>", unsafe_allow_html=True)
                        else:
                            with col2:
                                st.write("No negative phrases found.")

                        # Add space between the Top Phrases and the Phrase Mentions section
                        st.markdown("<br>", unsafe_allow_html=True)

                    else:
                        st.warning(f"No phrases found for the selected aspect: {selected_aspect}")
                st.divider()
                # Get review snippets with confidence score > 80
                reviews = load_table_data(f"""
                    SELECT SENTIMENT_TYPE, SENTIMENT_TEXT, START_INDEX, END_INDEX, CONFIDENCE_SCORE, REVIEW_TEXT
                    FROM PRODUCT_REVIEW_SNIPPET
                    WHERE PRODUCT_ID = {selected_product_id} 
                    AND ASPECT_NAME = '{selected_aspect}' AND CONFIDENCE_SCORE > .8
                    ORDER BY CONFIDENCE_SCORE DESC
                """)

                if not reviews.empty:
                    positive_count = reviews[reviews['SENTIMENT_TYPE'] == 'positive'].shape[0]
                    negative_count = reviews[reviews['SENTIMENT_TYPE'] == 'negative'].shape[0]
                    # Add space between the Top Phrases and the Phrase Mentions section
                    st.markdown("<br>", unsafe_allow_html=True)  # This adds a line break
                    # Add space between the Top Phrases and the Phrase Mentions section
                    st.markdown("<br>", unsafe_allow_html=True)  # This adds a line break
                    st.markdown(f"<div style='padding: 10px; background-color: lightgreen; color: black;'>**Positive Mentions:** {positive_count}</div>", unsafe_allow_html=True)
                    st.markdown(f"<div style='padding: 10px; background-color: darkred; color: white;'>**Negative Mentions:** {negative_count}</div>", unsafe_allow_html=True)

                    # Pagination: Show reviews with proper pagination and buttons
                    positive_button = st.button(f"Show Positive Reviews ({positive_count})")
                    negative_button = st.button(f"Show Negative Reviews ({negative_count})")

                    filtered_reviews = reviews
                    if positive_button:
                        filtered_reviews = reviews[reviews['SENTIMENT_TYPE'] == 'positive']
                    elif negative_button:
                        filtered_reviews = reviews[reviews['SENTIMENT_TYPE'] == 'negative']

                    st.subheader(f"Reviews for {selected_aspect}")

                    reviews_per_page = 10
                    total_reviews = len(filtered_reviews)
                    page = st.number_input("Select Page:", min_value=1, max_value=int(np.ceil(total_reviews / reviews_per_page)), step=1)

                    start_idx = (page - 1) * reviews_per_page
                    end_idx = start_idx + reviews_per_page

                    # Displaying reviews with a separator
                    for idx, review in filtered_reviews[start_idx:end_idx].iterrows():
                        sentiment_color = "#90EE90" if review['SENTIMENT_TYPE'] == 'positive' else "#8B0000"
                        text_color = "black" if review['SENTIMENT_TYPE'] == 'positive' else "white"
                        
                        # Highlighting the sentiment in the review text
                        highlighted_text = (
                            review['REVIEW_TEXT'][:review['START_INDEX']] +
                            f"<span style='background-color:{sentiment_color};font-weight:bold; color:{text_color};'>{review['SENTIMENT_TEXT']}</span>" +
                            review['REVIEW_TEXT'][review['END_INDEX']:]
                        )
                        
                        st.markdown(highlighted_text, unsafe_allow_html=True) 
                        # Adding a separator between review snippets
                        st.divider()
                else:
                    st.warning("No insights found for the selected aspect ")
            else:
                st.warning("No insights found for the selected product.")
