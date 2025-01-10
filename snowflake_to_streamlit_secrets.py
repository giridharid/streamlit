import streamlit as st
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt
import numpy as np

# Snowflake connection function
def create_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"]
    )

# Load data from Snowflake
def load_table_data(query, params=None):
    conn = create_snowflake_connection()
    try:
        return pd.read_sql(query, conn, params=params)
    finally:
        conn.close()

# Search hotels
def search_hotels(search_term):
    query = """
        SELECT PRODUCT_ID, HOTEL_NAME, CITY, STAR_RATING
        FROM EMT.PUBLIC.PRODUCT_LIST
        WHERE HOTEL_NAME ILIKE %s
    """
    return load_table_data(query, params=[f"%{search_term}%"])

# Get product insights
def get_product_insight(product_id):
    query = """
        SELECT AMENITIES_SCORE, LOCATION_SCORE, DINING_SCORE, GENERAL_SCORE,
               CLEANLINESS_SCORE, STAFF_SCORE, VALUE_FOR_MONEY_SCORE, ROOM_SCORE,
               PRODUCT_SUMMARY, TOP_EMOTION_1, TOP_EMOTION_2, TOP_EMOTION_3, OVERALL_SCORE
        FROM EMT.PUBLIC.PRODUCT_INSIGHT
        WHERE PRODUCT_ID = %s
    """
    return load_table_data(query, params=[product_id])

# Get review snippets (no limit on rows)
def get_review_snippets(product_id, aspect_name):
    query = """
        SELECT SENTIMENT_TYPE, SENTIMENT_TEXT, START_INDEX, END_INDEX, CONFIDENCE_SCORE, REVIEW_TEXT
        FROM EMT.PUBLIC.PRODUCT_REVIEW_SNIPPET
        WHERE PRODUCT_ID = %s AND ASPECT_NAME = %s
        ORDER BY CONFIDENCE_SCORE DESC
    """
    return load_table_data(query, params=[product_id, aspect_name])

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
                st.write(f"**Product ID:** {selected_product_id}")

                # Display top emotions
                top_emotions = [
                    insights.iloc[0]["TOP_EMOTION_1"], 
                    insights.iloc[0]["TOP_EMOTION_2"], 
                    insights.iloc[0]["TOP_EMOTION_3"]
                ]
                top_emotions = [emotion for emotion in top_emotions if pd.notna(emotion)]

                if top_emotions:
                    st.subheader("Top Emotions")
                    st.write(", ".join(top_emotions))
                else:
                    st.write("No emotions available.")

                # Display aspect scores dynamically
                st.subheader("Aspect Scores")
                aspect_columns = [col for col in insights.columns if col.endswith("_SCORE")]
                aspect_columns = [col for col in aspect_columns if col != "GENERAL_SCORE"]

                valid_scores = insights.iloc[0][aspect_columns].dropna()
                aspect_names = [
                    col.replace("_SCORE", "").replace("_", " ").capitalize() for col in valid_scores.index
                ]

                if len(valid_scores) == len(aspect_names):
                    aspect_scores = pd.DataFrame({
                        "Aspect": aspect_names,
                        "Score": valid_scores.values
                    })

                    fig, ax = plt.subplots()
                    bars = ax.bar(aspect_scores["Aspect"], aspect_scores["Score"], 
                                  color=plt.cm.viridis(np.linspace(0, 1, len(aspect_scores))))
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

                # Select aspect from Aspect List table
                aspects = get_aspect_list()
                selected_aspect = st.selectbox("Select an Aspect:", aspects["ASPECT_NAME"].tolist())

                if selected_aspect:
                    reviews = get_review_snippets(selected_product_id, selected_aspect)

                    if not reviews.empty:
                        positive_count = reviews[reviews['SENTIMENT_TYPE'] == 'positive'].shape[0]
                        negative_count = reviews[reviews['SENTIMENT_TYPE'] == 'negative'].shape[0]

                        st.markdown(f"<div style='padding: 10px; background-color: lightgreen; color: black;'>"
                                    f"**Positive Mentions:** {positive_count}</div>", unsafe_allow_html=True)
                        st.markdown(f"<div style='padding: 10px; background-color: darkred; color: white;'>"
                                    f"**Negative Mentions:** {negative_count}</div>", unsafe_allow_html=True)

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
                        page = st.number_input("Select Page:", min_value=1, 
                                               max_value=int(np.ceil(total_reviews / reviews_per_page)), step=1)

                        start_idx = (page - 1) * reviews_per_page
                        end_idx = start_idx + reviews_per_page

                        for idx, review in filtered_reviews[start_idx:end_idx].iterrows():
                            sentiment_color = "#90EE90" if review['SENTIMENT_TYPE'] == 'positive' else "#8B0000"
                            text_color = "black" if review['SENTIMENT_TYPE'] == 'positive' else "white"

                            highlighted_text = (
                                review['REVIEW_TEXT'][:review['START_INDEX']] +
                                f"<span style='background-color:{sentiment_color};font-weight:bold; color:{text_color};'>"
                                f"{review['SENTIMENT_TEXT']}</span>" +
                                review['REVIEW_TEXT'][review['END_INDEX']:]
                            )
                            st.markdown(highlighted_text, unsafe_allow_html=True)
                    else:
                        st.warning("No reviews found for this aspect.")
            else:
                st.warning("No insights found for the selected product.")
