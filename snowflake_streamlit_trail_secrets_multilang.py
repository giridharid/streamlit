import pandas as pd
import json
import html
from google.cloud import translate_v2 as translate

def translate_text(text, target_language):
    """
    Translates the given text to the target language using Google Translate API.
    """
    if not text or text.strip() == "":
        return ""
    
    translate_client = translate.Client()
    result = translate_client.translate(text, target_language=target_language)
    return html.unescape(result["translatedText"])

def process_file(file_path, product_name=None):
    """
    Reads a CSV or JSON file, translates sentiment_text and review_text
    into Hindi and Tamil, and creates a new file with translated text.
    """
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    elif file_path.endswith(".json"):
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            df = pd.DataFrame(data)
    else:
        raise ValueError("Unsupported file format. Use CSV or JSON.")
    
    # Ensure required columns exist
    required_columns = {"product_name", "sentiment_text", "review_text"}
    if not required_columns.issubset(df.columns):
        raise ValueError("Missing required columns in the file.")
    
    # Filter for specific product if provided
    if product_name:
        df = df[df["product_name"].str.lower() == product_name.lower()]
    
    if df.empty:
        raise ValueError("No matching product found.")
    
    # Translate sentiment_text and review_text
    df["sentiment_text_hi"] = df["sentiment_text"].apply(lambda x: translate_text(x, "hi"))
    df["sentiment_text_ta"] = df["sentiment_text"].apply(lambda x: translate_text(x, "ta"))
    df["review_text_hi"] = df["review_text"].apply(lambda x: translate_text(x, "hi"))
    df["review_text_ta"] = df["review_text"].apply(lambda x: translate_text(x, "ta"))
    
    # Save to a new file
    output_file = file_path.replace(".csv", "_translated.csv").replace(".json", "_translated.json")
    if file_path.endswith(".csv"):
        df.to_csv(output_file, index=False, encoding="utf-8")
    else:
        df.to_json(output_file, orient="records", force_ascii=False, indent=4)
    
    return output_file

# Example Usage
if __name__ == "__main__":
    input_file = "reviews.csv"  # Replace with actual file path
    product_to_process = "Example Product"  # Set to None to process all products
    output_path = process_file(input_file, product_to_process)
    print(f"Translated file saved at: {output_path}")
