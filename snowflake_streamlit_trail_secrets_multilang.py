import streamlit as st
import pandas as pd
import snowflake.connector
import numpy as np
import re
import matplotlib.pyplot as plt

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

# Load data from Snowflake with encoding fixes
def load_table_data(query):
    conn = create_snowflake_connection()
    try:
        df = pd.read_sql(query, conn)
        
        # Fix encoding issues (UTF-8 decoding to prevent junk characters)
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).apply(lambda x: x.encode('utf-8', 'ignore').decode('utf-8', 'ignore') if x else x)
        
        return df
    finally:
        conn.close()

# Function to highlight the entire sentence containing sentiment text
def highlight_full_sentence(text, sentiment, sentiment_type):
    if not isinstance(text, str) or not isinstance(sentiment, str):
        return text  

    # Fix encoding before highlighting
    text = text.encode('utf-8', 'ignore').decode('utf-8', 'ignore')
    sentiment = sentiment.encode('utf-8', 'ignore').decode('utf-8', 'ignore')

    sentiment_color = "#90EE90" if sentiment_type == 'positive' else "#8B0000"
    text_color = "black" if sentiment_type == 'positive' else "white"

    if sentiment.lower() not in text.lower():
        return text  

    sentences = re.split(r'(?<=[.!?])\s+', text)
    for i, sentence in enumerate(sentences):
        if sentiment.lower() in sentence.lower():
            highlighted_sentence = f"<span style='background-color:{sentiment_color}; color:{text_color}; font-weight:bold;'>{sentence}</span>"
            sentences[i] = highlighted_sentence
            return " ".join(sentences)

    return text  

# **UI starts here**
st.title("Hotel Insights Dashboard")

# **Search hotels**
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
           # **Display product insights**
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
                st.divider()

                # Display top emotions
                st.subheader("Top Emotions")
                top_emotions = [insights.iloc[0]["TOP_EMOTION_1"], insights.iloc[0]["TOP_EMOTION_2"], insights.iloc[0]["TOP_EMOTION_3"]]
                top_emotions = [emotion for emotion in top_emotions if pd.notna(emotion)]

                if top_emotions:
                    st.write(", ".join(top_emotions))
                else:
                    st.write("No emotions available.")
                st.divider()

               # Display aspect scores dynamically
                st.subheader("Aspect Scores")
                aspect_columns = [col for col in insights.columns if col.endswith("_SCORE") and col != "GENERAL_SCORE"]
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

                # **Aspect Selection**
                aspects = load_table_data("SELECT DISTINCT ASPECT_NAME FROM ASPECT_LIST WHERE ASPECT_NAME != 'General'")
                selected_aspect = st.selectbox("Select an Aspect:", aspects["ASPECT_NAME"].tolist())
                
                if selected_aspect:
                    # Display top phrases
                    top_phrases = load_table_data(f"""
                        SELECT POSITIVE_PHRASES, NEGATIVE_PHRASES
                        FROM PRODUCT_ASPECT_TOP_PHRASE
                        WHERE PRODUCT_ID = {selected_product_id} AND ASPECT = '{selected_aspect}'
                    """)

                    if not top_phrases.empty:
                        st.subheader("Top Phrases")
                        positive_phrases = top_phrases['POSITIVE_PHRASES'].iloc[0]
                        negative_phrases = top_phrases['NEGATIVE_PHRASES'].iloc[0]

                        col1, col2 = st.columns([1, 1])

                        # Display positive phrases
                        if positive_phrases:
                            positive_phrases = positive_phrases.split(",")
                            with col1:
                                st.write("**Positive Phrases**")
                                for phrase in positive_phrases:
                                    st.markdown(f"<div style='background-color:#d4edda;padding:10px;'><b>{phrase.strip()}</b></div>", unsafe_allow_html=True)
                        else:
                            with col1:
                                st.write("No positive phrases found.")

                        # Display negative phrases
                        if negative_phrases:
                            negative_phrases = negative_phrases.split(",")
                            with col2:
                                st.write("**Negative Phrases**")
                                for phrase in negative_phrases:
                                    st.markdown(f"<div style='background-color:#f8d7da;padding:10px;'><b>{phrase.strip()}</b></div>", unsafe_allow_html=True)
                        else:
                            with col2:
                                st.write("No negative phrases found.")
                    st.divider()

                if selected_aspect:
                    # **Load Multi-Language Reviews**
                    reviews = load_table_data(f"""
                        SELECT ROW_NUMBER() OVER (ORDER BY CONFIDENCE_SCORE, START_INDEX DESC) AS ROW_NUM, 
                               SENTIMENT_TYPE, SENTIMENT_TEXT, SENTIMENT_TEXT_HI, SENTIMENT_TEXT_TA, SENTIMENT_TEXT_TE, SENTIMENT_TEXT_KN, SENTIMENT_TEXT_ES, SENTIMENT_TEXT_FR, SENTIMENT_TEXT_IW,
                               REVIEW_TEXT, REVIEW_TEXT_HI, REVIEW_TEXT_TA, REVIEW_TEXT_TE, REVIEW_TEXT_KN, REVIEW_TEXT_ES, REVIEW_TEXT_FR, REVIEW_TEXT_IW,
                               CONFIDENCE_SCORE
                        FROM PRODUCT_MULTI_LANG_REVIEW_SNIPPET
                        WHERE PRODUCT_ID = {selected_product_id}
                        AND ASPECT_NAME = '{selected_aspect}'
                        AND CONFIDENCE_SCORE > 0.8
                        ORDER BY CONFIDENCE_SCORE, START_INDEX DESC
                    """)
        
                    if not reviews.empty:
                        positive_count = reviews[reviews['SENTIMENT_TYPE'] == 'positive'].shape[0]
                        negative_count = reviews[reviews['SENTIMENT_TYPE'] == 'negative'].shape[0]
        
                        st.markdown(f"<div style='padding: 10px; background-color: lightgreen; color: black;'>**Positive Mentions:** {positive_count}</div>", unsafe_allow_html=True)
                        st.markdown(f"<div style='padding: 10px; background-color: darkred; color: white;'>**Negative Mentions:** {negative_count}</div>", unsafe_allow_html=True)
                        st.divider()
        
                        reviews_per_page = st.selectbox("Reviews per page:", options=[10, 25, 40], index=1)
                        total_reviews = len(reviews)
                        max_page = int(np.ceil(total_reviews / reviews_per_page))
                        page = st.number_input("Select Page:", min_value=1, max_value=max_page, step=1)
        
                        start_idx = (page - 1) * reviews_per_page
                        end_idx = start_idx + reviews_per_page
                        reviews_batch = reviews.iloc[start_idx:end_idx]
        
                        st.divider()
        
                        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs(["English", "Hindi", "Tamil", "Telugu", "Kannada", "Spanish", "French", "Hebrew"])
        
                        for tab, lang, review_col, sentiment_col in zip(
                                [tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8],
                                ["English", "Hindi", "Tamil", "Telugu", "Kannada", "Spanish", "French", "Hebrew"],
                                ["REVIEW_TEXT", "REVIEW_TEXT_HI", "REVIEW_TEXT_TA", "REVIEW_TEXT_TE", "REVIEW_TEXT_KN", "REVIEW_TEXT_ES", "REVIEW_TEXT_FR", "REVIEW_TEXT_IW"],
                                ["SENTIMENT_TEXT", "SENTIMENT_TEXT_HI", "SENTIMENT_TEXT_TA", "SENTIMENT_TEXT_TE", "SENTIMENT_TEXT_KN", "SENTIMENT_TEXT_ES", "SENTIMENT_TEXT_FR", "SENTIMENT_TEXT_IW"]):
        
                            with tab:
                                st.subheader(f"Reviews in {lang} ({selected_aspect})")
                                for _, review in reviews_batch.iterrows():
                                    highlighted_text = highlight_full_sentence(review[review_col], review[sentiment_col], review['SENTIMENT_TYPE'])
                                    st.markdown(f"<div style='padding:10px;'><b>{review['ROW_NUM']}. </b>{highlighted_text}</div>", unsafe_allow_html=True)
                                    st.divider()
