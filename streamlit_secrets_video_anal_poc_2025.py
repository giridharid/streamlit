!pip install --upgrade pip
import streamlit as st
import pandas as pd
import snowflake.connector
import streamlit_wordcloud as wordcloud
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
    if "youtube.com" in video_url or "youtu.be" in video_url:
        components.iframe(video_url.replace("watch?v=", "embed/"), height=315)
    else:
        st.error("Invalid YouTube URL")

# Create a word cloud from filtered keywords
def generate_wordcloud(filtered_keywords):
    if filtered_keywords:
        wordcloud_streamlit = wordcloud.visualize(
            data=dict(pd.Series(filtered_keywords).value_counts()),
            max_words=100,
            height=400,
            width=800,
            background_color="white"
        )
        st.markdown(wordcloud_streamlit, unsafe_allow_html=True)
    else:
        st.warning("No relevant keywords to display in the word cloud.")

# Filter keywords based on predefined categories
def filter_keywords(text, categories):
    keywords = []
    for category, terms in categories.items():
        for term in terms:
            if term in text.lower():
                keywords.append(term)
    return keywords

# Application Layout
st.set_page_config(
    page_title="YouTube Video Analysis Tool",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📊 YouTube Video Analysis Tool")
st.markdown("Analyze YouTube videos for keywords, generate word clouds, and navigate directly to snippets.")

# Expanded keyword categories for smartphones
expanded_reasons = {
    "Affordability": ["expensive", "cheaper", "cost", "budget", "price", "affordable", "overpriced", "too costly", "cost-effective", "discount", "monthly payments", "subscription", "financing", "trade-in", "cashback", "offers", "price hike", "deal", "promo"],
    "Camera": ["camera", "photo quality", "image processing", "hdr", "night mode", "zoom", "megapixels", "pro mode", "portrait mode", "low light", "optical stabilization", "ultra-wide", "telephoto", "macro mode", "camera software", "AI camera", "autofocus", "video quality"],
    "Display": ["screen", "display", "oled", "lcd", "refresh rate", "resolution", "amoled", "super retina", "touch response", "screen size", "hdr display", "pwm flicker", "color accuracy", "adaptive refresh rate", "always-on display", "sunlight visibility", "screen burn-in"],
    "Battery": ["battery", "battery life", "fast charging", "wireless charging", "battery drain", "power saving", "battery health", "charger", "mah", "charging speed", "heat during charging", "battery optimization", "longer battery life", "standby time", "powerbank"],
    "Performance": ["speed", "performance", "processor", "chip", "lag", "slow", "overheating", "thermal", "benchmark", "multitasking", "frame drop", "gaming performance", "gpu", "cpu", "snapdragon", "apple silicon", "ram management", "ios optimization"],
    "Design or Experience": ["design", "build quality", "aesthetic", "feel", "user experience", "material", "weight", "size", "ergonomic", "form factor", "glass back", "aluminum frame", "premium feel", "haptic feedback", "curved screen", "notch", "punch-hole", "bezels"],
    "Apps": ["apps", "play store", "app store", "compatibility", "app crashing", "third-party apps", "bloatware", "app restrictions", "apk", "exclusive apps", "google apps", "apple apps", "side-loading", "app support", "app permissions", "play store policies"],
    "Features": ["features", "customization", "gesture controls", "widgets", "face id", "fingerprint sensor", "ai enhancements", "new update", "software improvements", "reverse charging", "split screen", "s pen", "foldable display", "stylus support", "dex mode"],
    "Messaging & Communication": ["whatsapp", "facetime", "imessage", "sms", "video call", "messaging", "google messages", "green bubble", "blue bubble", "end-to-end encryption", "group chats", "telegram", "discord", "mms", "rsc messaging", "signal", "snapchat"],
    "Ecosystem & Integration": ["google services", "apple ecosystem", "icloud", "android sync", "airdrop", "samsung wallet", "google wallet", "continuity", "handoff", "apple watch", "google assistant", "siri", "carplay", "android auto", "multi-device sync"],
    "Software Updates & Longevity": ["updates", "software support", "os updates", "security patch", "android versions", "planned obsolescence", "long term updates", "kernel update", "beta software", "stable updates", "one ui updates", "ios updates", "security vulnerabilities"],
    "Privacy & Security": ["privacy", "security", "encryption", "open source", "data collection", "permissions", "apple security", "google tracking", "two-factor authentication", "lock screen", "app tracking transparency", "vpn support", "ad tracking", "face unlock", "fingerprint unlock", "passcode protection"],
    "Regional & Cultural Differences": ["us", "europe", "regional", "country", "market", "brand perception", "availability", "pricing differences", "network bands", "import fees", "regional pricing", "limited edition", "model exclusivity", "carrier support", "5g bands", "wifi calling"],
    "iOS to Android": ["switching to android", "moved to android", "left iphone", "quit iphone", "got an android", "ditched iphone", "tired of iphone", "switched from iphone", "android better than ios", "apple too expensive", "needed customization", "more device choices", "wanted a bigger battery", "hated apple ecosystem", "left because of restrictions", "app sideloading", "better gaming experience"],
    "Android to iOS": ["switching to iphone", "moved to iphone", "left android", "quit android", "got an iphone", "ditched android", "tired of android", "switched from android", "ios better than android", "wanted better security", "better camera experience", "ios updates are better", "smoother UI", "better resale value", "apple ecosystem", "wanted iMessage and FaceTime", "better privacy features"]
}

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
        filtered_keywords = filter_keywords(combined_text, expanded_reasons)

        st.subheader("Word Cloud")
        st.markdown("Visualize the most frequently occurring keywords in the video transcription.")
        generate_wordcloud(filtered_keywords)

        st.subheader("Snippet Keywords")
        st.markdown("Click on a keyword to view and play the corresponding video snippet.")

        # Display clickable keywords in a grid
        unique_words = set(filtered_keywords)
        col1, col2, col3 = st.columns(3)
        cols = [col1, col2, col3]

        for idx, word in enumerate(unique_words):
            col = cols[idx % 3]
            if col.button(word):
                snippet_rows = snippets_df[snippets_df['TRANSCRIPTION_TEXT'].str.contains(word, na=False)]
                if not snippet_rows.empty:
                    snippet = snippet_rows.iloc[0]
                    st.write(f"Playing snippet containing '{word}':")
                    st.write(f"**Start Time**: {snippet['START_TIME']} seconds, **End Time**: {snippet['END_TIME']} seconds")
                    snippet_video_url = f"{video_details['VIDEO_URL']}?start={int(snippet['START_TIME'])}&end={int(snippet['END_TIME'])}"
                    render_video(snippet_video_url)
                else:
                    st.warning(f"No snippet found containing the keyword '{word}'.")
    else:
        st.warning("No snippets available for this video.")
