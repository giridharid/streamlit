import streamlit as st
from elasticsearch import Elasticsearch
import requests
#from bs4 import BeautifulSoup
import random

import re


# Elasticsearch connection using Streamlit secrets
client = Elasticsearch(
    st.secrets["elasticsearch"]["endpoint"],
    api_key=st.secrets["elasticsearch"]["api_key"]
)

# Fetch index name from secrets
index_name = st.secrets["elasticsearch"]["index_name"]

# Synonyms for aspects and quality words
feature_synonyms = {
    "amenities": ["amenities", "amenity", "facilities", "facility", "features", "comforts", "offerings", "provisions", "services"],
    "cleanliness": ["cleanliness", "clean", "hygiene", "sanitation", "tidiness", "neatness"],
    "location": ["location", "proximity", "area", "neighborhood", "vicinity", "surroundings", "accessibility"],
    "dining": ["dining", "restaurant", "meal options", "food services", "culinary offerings", "meals"],
    "staff": ["staff", "personnel", "team", "employees", "workforce", "attendants", "service"],
    "value_for_money": ["value for money", "affordability", "worth", "reasonable pricing", "budget friendliness"]
}

quality_seeds = {
    "excellent": ["excellent", "great", "outstanding", "superb", "amazing", "delightful"],
    "good": ["good", "nice", "decent", "satisfactory", "fine", "pleasant"],
    "average": ["average", "okay", "acceptable", "mediocre", "fair"]
}

# Lemmatization
def lemmatize_query(query):
    """Simplistic lemmatization for now."""
    return query.lower()

# Synonym Matching
def match_synonyms(query, synonyms_dict):
    matched_aspects = set()
    query_lower = query.lower()
    for aspect, synonyms in synonyms_dict.items():
        for synonym in synonyms:
            if synonym in query_lower:
                matched_aspects.add(aspect)
                break
    return matched_aspects

# Map scores for aspects and overall
def map_scores(aspect_scores=None, quality_word=None):
    if quality_word:
        if quality_word.lower() in quality_seeds["excellent"]:
            return {"gte": 80}
        elif quality_word.lower() in quality_seeds["good"]:
            return {"gte": 79}
        elif quality_word.lower() in quality_seeds["average"]:
            return {"gte": 60}
        else:
            return None

    if aspect_scores:
        aspect_descriptors = {}
        for aspect, score in aspect_scores.items():
            if score >= 80:
                aspect_descriptors[aspect] = "excellent"
            elif score >= 70:
                aspect_descriptors[aspect] = "good"
            else:
                aspect_descriptors[aspect] = "average"
        return aspect_descriptors

# Price and Star Rating Extraction
def extract_price_star_rating_currency(query):
    normalized_query = query.lower().replace("k", "000")
    range_match = re.search(r"between\s+(\d+)\s+(and|-)\s+(\d+)", normalized_query)
    if range_match:
        min_price = int(range_match.group(1))
        max_price = int(range_match.group(3))
    else:
        min_price = None
        single_price_match = re.search(r"(under|below)\s+(\d+)", normalized_query)
        max_price = int(single_price_match.group(2)) if single_price_match else None

    currency_match = re.search(r"(\d+)\s*(usd|inr)", normalized_query)
    currency = currency_match.group(2).lower() if currency_match else "inr"

    star_rating_match = re.search(r"(\d+)[\s*-]star", normalized_query)
    star_rating = int(star_rating_match.group(1)) if star_rating_match else None

    return min_price, max_price, currency, star_rating

# TripAdvisor Data Fetch
def fetch_tripadvisor_data(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        reviews = [review.text.strip() for review in soup.find_all('q', class_='IRsGHoPm')[:3]]
        amenities = [amenity.text.strip() for amenity in soup.find_all('div', class_='bUmsU f ME H3 _c')]

        return {
            "reviews": reviews if reviews else "No reviews found.",
            "amenities": amenities if amenities else "No amenities listed."
        }
    except requests.exceptions.RequestException as e:
        return {"error": f"Request failed: {str(e)}"}
    except Exception as e:
        return {"error": f"General error: {str(e)}"}

# Elasticsearch Query Logic
def retrieve_hotels(query):
    query = lemmatize_query(query)
    matched_aspects = match_synonyms(query, feature_synonyms)
    query_words = query.split()
    quality_words = [word for word in query_words if map_scores(quality_word=word)]

    aspect_conditions = []
    processed_aspects = set()
    for aspect in matched_aspects:
        for word in quality_words:
            quality_range = map_scores(quality_word=word)
            if quality_range and aspect not in processed_aspects:
                aspect_conditions.append({"range": {f"{aspect}_score": quality_range}})
                processed_aspects.add(aspect)
                break

    min_price, max_price, currency, star_rating = extract_price_star_rating_currency(query)
    price_field = "price_usd" if currency == "usd" else "price_inr"

    must_clauses = [{"range": {price_field: {"gte": min_price, "lte": max_price}}} if max_price else {}]
    if star_rating:
        must_clauses.append({"term": {"star_rating": star_rating}})

    for condition in aspect_conditions:
        must_clauses.append(condition)

    try:
        response = client.search(
            index=index_name,
            body={
                "query": {
                    "bool": {
                        "must": must_clauses
                    }
                },
                "size": 5
            }
        )
        return response['hits']['hits']
    except Exception as e:
        return {"error": str(e)}

# Streamlit UI
st.set_page_config(page_title="Enhanced Hotel Search", layout="wide", page_icon="🏨")
st.header("🏨 Enhanced Hotel Search with Aspect-Based Filtering")
st.markdown(
    """
    **Examples of queries:**
    - _"Excellent clean and great dining 4-star hotels in Delhi between 5000 and 6000 INR"_
    - _"Affordable 5-star hotels in Mumbai with superb location and staff under 7000 INR"_
    """
)
st.write("---")

# Input Query Section
st.subheader("Search for Hotels")
st.write("Enter your search query below, and let our AI-powered search provide the best options.")
query = st.text_input("Enter your query:", placeholder="E.g., Excellent clean 4-star hotels in Delhi under 6000 INR")

# A banner or illustrative image to enhance the visual appeal
topics = ["hotel", "travel", "beach", "luxury"]
random_topic = random.choice(topics)
#banner_image_url = f"https://images.unsplash.com/photo-1506748686214-e9df14d4d9d0?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&q=80&w=1080"
#st.image(banner_image_url, use_container_width=True)

if query:
    results = retrieve_hotels(query)
    
    if isinstance(results, list):
        for hit in results:
            source = hit["_source"]
            st.subheader(source.get("hotel_name", "Unknown Hotel"))
            st.write(f"**City:** {source.get('city', 'N/A')} | **Price:** {source.get('price_inr', 'N/A')} INR / {source.get('price_usd', 'N/A')} USD")

            # Ratings and aspects
            aspect_scores = {
                "Cleanliness": source.get("cleanliness_score"),
                "Amenities": source.get("amenities_score"),
                "Location": source.get("location_score"),
                "Dining": source.get("dining_score"),
                "Staff": source.get("staff_score"),
                "Value for Money": source.get("value_for_money_score"),
                "Overall": source.get("overall_score")
            }
            st.write("**Ratings:**")
            for aspect, score in aspect_scores.items():
                if score:
                    # Determine the rating based on the score
                    if score >= 80:
                        rating = "Excellent"
                        emoji = "✅"
                    elif score >= 70:
                        rating = "Good"
                        emoji = "👍"
                    else:
                        rating = "Average"
                        emoji = "⚠️"
                    # Display the aspect with the rating and emoji
                    st.write(f"  - {aspect}: {rating} {emoji}")

            # Product summary
            st.write(f"**Product Summary:** {source.get('summary', 'No summary available')}")


            st.write(f"TripAdvisor link. View more here: {source.get('tripadvisor_link', 'No tripadvisor link available')}")


            st.write("---")
    else:
        st.error(results.get("error", "Error occurred during the search."))

        st.error(results.get("error", "Error occurred during the search."))
