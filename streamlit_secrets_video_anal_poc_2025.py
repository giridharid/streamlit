import streamlit as st
import pandas as pd
import snowflake.connector
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import streamlit.components.v1 as components

# Snowflake Connection
@st.cache_resource
def create_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

conn = create_snowflake_connection()

# Fetch video metadata from Snowflake
def fetch_video_metadata():
    query = "SELECT VIDEO_ID, TITLE, DESCRIPTION, VIDEO_URL FROM VIDEO.PUBLIC.VIDEO_METADATA"
    return pd.read_sql(query, conn)

# Fetch video snippet data for analysis
def fetch_video_snippets(video_id):
    query = f"""
    SELECT TRANSCRIPTION_TEXT, START_TIME, END_TIME 
    FROM VIDEO.PUBLIC.VIDEO_SNIPPET 
    WHERE VIDEO_ID = '{video_id}'"""
    return pd.read_sql(query, conn)

# Render YouTube Videos
def render_video(video_url):
    components.iframe(video_url, height=315)

# Create a word cloud from transcription text
def generate_wordcloud(text):
    wordcloud = WordCloud(width=800, height=400, background_color='white', colormap='viridis').generate(text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    st.pyplot(plt)

# Application Layout
st.set_page_config(
    page_title="YouTube Video Analysis Tool",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📊 YouTube Video Analysis Tool")
st.markdown("Analyze YouTube videos for keywords, generate word clouds, and navigate directly to snippets.")

# Sidebar for video selection
st.sidebar.header("🎥 Select a Video")
video_metadata = fetch_video_metadata()
selected_video = st.sidebar.selectbox("Choose a Video", video_metadata['TITLE'])

if selected_video:
    video_details = video_metadata[video_metadata['TITLE'] == selected_video].iloc[0]

    # Main content
    st.subheader(f"**{video_details['TITLE']}**")
    st.write(video_details['DESCRIPTION'])
    render_video(video_details['VIDEO_URL'])

    # Keyword Analysis Section
    st.markdown("---")
    st.header("🔑 Keyword Analysis")
    snippets_df = fetch_video_snippets(video_details['VIDEO_ID'])

    if not snippets_df.empty:
        combined_text = " ".join(snippets_df['TRANSCRIPTION_TEXT'])

        st.subheader("Word Cloud")
        st.markdown("Visualize the most frequently occurring keywords in the video transcription.")
        generate_wordcloud(combined_text)

        st.subheader("Snippet Keywords")
        st.markdown("Click on a keyword to view and play the corresponding video snippet.")

        # Display clickable keywords in a grid
        words = combined_text.split()
        unique_words = set(words)
        col1, col2, col3 = st.columns(3)
        cols = [col1, col2, col3]

        for idx, word in enumerate(unique_words):
            col = cols[idx % 3]
            if col.button(word):
                snippet = snippets_df[snippets_df['TRANSCRIPTION_TEXT'].str.contains(word, na=False)].iloc[0]
                st.write(f"Playing snippet containing '{word}':")
                st.write(f"**Start Time**: {snippet['START_TIME']} seconds, **End Time**: {snippet['END_TIME']} seconds")
                snippet_video_url = f"{video_details['VIDEO_URL']}?start={int(snippet['START_TIME'])}&end={int(snippet['END_TIME'])}"
                render_video(snippet_video_url)
    else:
        st.warning("No snippets available for this video.")
