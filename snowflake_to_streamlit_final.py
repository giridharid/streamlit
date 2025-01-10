import streamlit as st
import pandas as pd
import snowflake.connector
import configparser
import matplotlib.pyplot as plt
import numpy as np

# Snowflake connection function
def create_snowflake_connection():
    config = configparser.ConfigParser()
    config.read("parameter.txt")

    user = config.get("Snowflake_connector", "USER")
    password = config.get("Snowflake_connector", "PASSWORD")
    account = config.get("Snowflake_connector", "ACCOUNT")

    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account
    )

# Load data from Snowflake
def load_table_data(query):
    conn = create_snowflake_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()

# Search hotels
def search_hotels(search_term):
    query = f"""
        SELECT PRODUCT_ID, HOTEL_NAME, CITY, STAR_RATING
        FROM EMT.PUBLIC.PRODUCT_LIST
        WHERE HOTEL_NAME ILIKE '%{search_term}%'
    """
    return load_table_data(query)

# Get product insights
def get_product_insight(product_id):
    query = f"""
        SELECT AMENITIES_SCORE, LOCATION_SCORE, DINING_SCORE, GENERAL_SCORE,
               CLEANLINESS_SCORE, STAFF_SCORE, VALUE_FOR_MONEY_SCORE, ROOM_SCORE,
               PRODUCT_SUMMARY, TOP_EMOTION_1, TOP_EMOTION_2, TOP_EMOTION_3, OVERALL_SCORE
        FROM EMT.PUBLIC.PRODUCT_INSIGHT
        WHERE PRODUCT_ID = {product_id}
    """
    return load_table_data(query)

# Get review snippets (no limit on rows)
def get_review_snippets(product_id, aspect_name):
    query = f"""
        SELECT SENTIMENT_TYPE, SENTIMENT_TEXT, START_INDEX, END_INDEX, CONFIDENCE_SCORE, REVIEW_TEXT
        FROM EMT.PUBLIC.PRODUCT_REVIEW_SNIPPET
        WHERE PRODUCT_ID = {product_id} AND ASPECT_NAME = '{aspect_name}'
        ORDER BY CONFIDENCE_SCORE DESC
    """
    return load_table_data(query)

# Get aspect list excluding "General"
def get_aspect_list():
    query = "SELECT DISTINCT ASPECT_NAME FROM EMT.PUBLIC.ASPECT_LIST"
    aspects = load_table_data(query)
    return aspects[aspects["ASPECT_NAME"].str.lower() != "general"]

# UI starts here
st.title("Hotel Insights Dashboard")

# Search hotels
search_term = st.text_input("Search Hotels by Name:")
if search_term:
    hotels = search_hotels(search_term)
    if hotels.empty:
        st.warning("No hotels found for the search term.")
    else:
        # Convert the PRODUCT_ID to string to avoid any formatting in the table
        hotels["PRODUCT_ID"] = hotels["PRODUCT_ID"].astype(str)
        st.dataframe(hotels)
        selected_hotel = st.selectbox("Select a Hotel:", hotels["HOTEL_NAME"].tolist())

        if selected_hotel:
            selected_product_id = hotels.loc[hotels["HOTEL_NAME"] == selected_hotel, "PRODUCT_ID"].iloc[0]

            # Display product insights
            insights = get_product_insight(selected_product_id)
            if not insights.empty:
                st.subheader("Product Insights")
                st.write(f"**Overall Score:** {insights['OVERALL_SCORE'].iloc[0]}")
                st.write(f"**Summary:** {insights['PRODUCT_SUMMARY'].iloc[0]}")
                st.write(f"**Product ID:** {selected_product_id}")  # Displaying product_id as is

                # Conditionally display top emotions
                top_emotions = [insights.iloc[0]["TOP_EMOTION_1"], insights.iloc[0]["TOP_EMOTION_2"], insights.iloc[0]["TOP_EMOTION_3"]]
                top_emotions = [emotion for emotion in top_emotions if pd.notna(emotion)]

                if top_emotions:
                    st.subheader("Top Emotions")
                    st.write(", ".join(top_emotions))
                else:
                    st.write("No emotions available.")

                # Display aspect scores dynamically with colorful bars
                st.subheader("Aspect Scores")

                # Filter columns with "_SCORE" in their name dynamically
                aspect_columns = [col for col in insights.columns if col.endswith("_SCORE")]
                # Exclude 'GENERAL_SCORE' from the aspect columns
                aspect_columns = [col for col in aspect_columns if col != "GENERAL_SCORE"]

                # Get the corresponding aspect names
                aspect_names = [col.replace("_SCORE", "").replace("_", " ").capitalize() for col in aspect_columns]

                # Filter out any rows where the score is NaN
                valid_scores = insights.iloc[0][aspect_columns].dropna()

                # Ensure the lengths match before creating the DataFrame
                if len(valid_scores) == len(aspect_names):
                    # Create a DataFrame for the valid aspect scores
                    aspect_scores = pd.DataFrame({
                        "Aspect": aspect_names,
                        "Score": valid_scores.values
                    })

                    # Plot colorful bar chart using matplotlib
                    fig, ax = plt.subplots()
                    bars = ax.bar(aspect_scores["Aspect"], aspect_scores["Score"], color=plt.cm.viridis(np.linspace(0, 1, len(aspect_scores))))
                    ax.set_xlabel('Aspect')
                    ax.set_ylabel('Score')
                    ax.set_title('Aspect Scores')

                    # Add labels on top of the bars with rounded scores
                    for bar in bars:
                        yval = round(bar.get_height())  # Round the score to the nearest integer
                        ax.text(bar.get_x() + bar.get_width() / 2, yval + 0.1, yval, ha='center', va='bottom')

                    plt.xticks(rotation=45, ha='right')
                    st.pyplot(fig)
                else:
                    st.warning("Mismatch between aspect names and aspect scores.")

                # Select aspect from Aspect List table
                aspects = get_aspect_list()
                selected_aspect = st.selectbox("Select an Aspect:", aspects["ASPECT_NAME"].tolist())

                if selected_aspect:
                    reviews = get_review_snippets(selected_product_id, selected_aspect)

                    if not reviews.empty:
                        # Count positive and negative sentiments
                        positive_count = reviews[reviews['SENTIMENT_TYPE'] == 'positive'].shape[0]
                        negative_count = reviews[reviews['SENTIMENT_TYPE'] == 'negative'].shape[0]

                        # Display positive and negative counts in colored boxes
                        st.markdown(f"<div style='padding: 10px; background-color: lightgreen; color: black;'>**Positive Mentions:** {positive_count}</div>", unsafe_allow_html=True)
                        st.markdown(f"<div style='padding: 10px; background-color: darkred; color: white;'>**Negative Mentions:** {negative_count}</div>", unsafe_allow_html=True)

                        # Buttons to filter positive and negative reviews
                        positive_button = st.button(f"Show Positive Reviews ({positive_count})")
                        negative_button = st.button(f"Show Negative Reviews ({negative_count})")

                        # Filter reviews based on sentiment
                        filtered_reviews = reviews
                        if positive_button:
                            filtered_reviews = reviews[reviews['SENTIMENT_TYPE'] == 'positive']
                        elif negative_button:
                            filtered_reviews = reviews[reviews['SENTIMENT_TYPE'] == 'negative']

                        st.subheader(f"Reviews for {selected_aspect}")

                        # Pagination: Show 10 reviews at a time
                        reviews_per_page = 10
                        total_reviews = len(filtered_reviews)
                        page = st.number_input("Select Page:", min_value=1, max_value=int(np.ceil(total_reviews / reviews_per_page)), step=1)

                        start_idx = (page - 1) * reviews_per_page
                        end_idx = start_idx + reviews_per_page

                        # Show the reviews in the selected range
                        for idx, review in filtered_reviews[start_idx:end_idx].iterrows():
                            sentiment_color = "#90EE90" if review['SENTIMENT_TYPE'] == 'positive' else "#8B0000"  # Light Green for positive, Dark Red for negative
                            text_color = "black" if review['SENTIMENT_TYPE'] == 'positive' else "white"  # White text for negative reviews

                            highlighted_text = (
                                review['REVIEW_TEXT'][:review['START_INDEX']] +
                                f"<span style='background-color:{sentiment_color};font-weight:bold; color:{text_color};'>{review['SENTIMENT_TEXT']}</span>" +
                                review['REVIEW_TEXT'][review['END_INDEX']:]
                            )
                            st.markdown(highlighted_text, unsafe_allow_html=True)
                    else:
                        st.warning("No reviews found for this aspect.")
            else:
                st.warning("No insights found for the selected product.")
