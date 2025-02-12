import streamlit as st
import snowflake.connector

# Database connection

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )


# Query functions
def fetch_hotels_by_query(query):
    # Example: Query logic to fetch hotels based on cleanliness or amenities
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT PRODUCT_LIST.HOTEL_NAME,  PRODUCT_LIST.STAR_RATING,  PRODUCT_LIST.CITY,  PRODUCT_LIST.TRIPADVISOR_LINK,  PRODUCT_LIST.PRODUCT_ID
        FROM EMT.PUBLIC.PRODUCT_LIST 
        JOIN EMT.PUBLIC.PRODUCT_INSIGHT ON PRODUCT_LIST.PRODUCT_ID = PRODUCT_INSIGHT.PRODUCT_ID
        WHERE AMENITIES_SCORE > 80 OR CLEANLINESS_SCORE > 80
        LIMIT 10;
    """)
    results = cursor.fetchall()
    conn.close()
    return results

def fetch_detailed_info(product_id):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM EMT.PUBLIC.PRODUCT_INSIGHT WHERE PRODUCT_ID = {product_id}")
    insights = cursor.fetchone()
    cursor.execute(f"SELECT * FROM EMT.PUBLIC.PRODUCT_EMOTION WHERE PRODUCT_ID = {product_id}")
    emotions = cursor.fetchone()
    cursor.execute(f"SELECT * FROM EMT.PUBLIC.PRODUCT_ASPECT_TOP_PHRASE WHERE PRODUCT_ID = {product_id}")
    phrases = cursor.fetchone()
    cursor.execute(f"SELECT * FROM EMT.PUBLIC.PRODUCT_MULTI_LANG_REVIEW_SNIPPET WHERE PRODUCT_ID = {product_id}")
    snippets = cursor.fetchall()
    conn.close()
    return insights, emotions, phrases, snippets

# Initialize session state for conversation history
if "messages" not in st.session_state:
    st.session_state["messages"] = []

# Chat interface
st.title("Hotel Chatbot with Multilingual Insights")

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# User input
if user_input := st.chat_input("Ask me about hotels (e.g., 'hotels with excellent amenities')"):
    # Add user message to conversation history
    st.session_state.messages.append({"role": "user", "content": user_input})

    # Bot logic
    with st.chat_message("assistant"):
        st.markdown("Let me find the best hotels for you...")
        hotels = fetch_hotels_by_query(user_input)
        
        if hotels:
            response = "Here are some hotels matching your query:\n\n"
            for hotel in hotels:
                response += f"**{hotel[0]}** ({hotel[2]}): ⭐{hotel[1]}\n[TripAdvisor Link]({hotel[3]})\n\n"
            
            st.markdown(response)

            for hotel in hotels:
                if st.button(f"View details for {hotel[0]}", key=f"details-{hotel[4]}"):
                    insights, emotions, phrases, snippets = fetch_detailed_info(hotel[4])
                    
                    # Show detailed info in chat
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": f"Here are the details for **{hotel[0]}**:"
                    })
                    
                    # Display details
                    st.markdown("### Product Summary")
                    st.write(insights["PRODUCT_SUMMARY"])

                    st.markdown("### Product Insights")
                    st.json(insights)

                    st.markdown("### Top Emotions")
                    st.write(f"Emotion 1: {emotions['EMOTION1']}")
                    st.write(f"Emotion 2: {emotions['EMOTION2']}")
                    st.write(f"Emotion 3: {emotions['EMOTION3']}")

                    st.markdown("### Top Phrases")
                    st.write(f"Positive Phrases: {phrases['POSITIVE_PHRASES']}")
                    st.write(f"Negative Phrases: {phrases['NEGATIVE_PHRASES']}")

                    st.markdown("### Aspect Mentions by Language")
                    for snippet in snippets:
                        st.write(f"Language: {snippet['REVIEW_TEXT_HI']}")
                        st.write(f"Sentiment: {snippet['SENTIMENT_TEXT_HI']}")
        else:
            st.markdown("No hotels found matching your criteria.")
