"""
Text Preprocessing Module for IMDb Movie Reviews.
Adapted and enhanced from traditional NLP pipelines.
Handles HTML tags, punctuation normalization, lowercasing, and whitespace stripping.
"""

import re
from typing import List, Union

# Compiled regex patterns for high-performance text sanitation
HTML_TAG_REGEX = re.compile(r"<[^>]+>")
URL_REGEX = re.compile(r"https?://\S+|www\.\S+")
NON_ALPHANUM_REGEX = re.compile(r"[^a-zA-Z0-9\s!?.,'\"]")
EXTRA_WHITESPACE_REGEX = re.compile(r"\s+")


def clean_text(text: Union[str, float, None]) -> str:
    """
    Cleans raw review text for TF-IDF feature extraction.
    
    Steps:
      1. Converts None or non-string values to empty string.
      2. Strips HTML markup (e.g. <br />, <i>, etc.).
      3. Strips raw URLs.
      4. Strips non-standard characters while preserving core sentiment punctuation.
      5. Converts to lower case.
      6. Normalizes consecutive whitespace.
      
    Args:
        text: Raw review string or text value.
        
    Returns:
        Cleaned, normalized text string.
    """
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)

    # Remove HTML tags
    cleaned = HTML_TAG_REGEX.sub(" ", text)
    # Remove URLs
    cleaned = URL_REGEX.sub(" ", cleaned)
    # Remove unusual characters
    cleaned = NON_ALPHANUM_REGEX.sub(" ", cleaned)
    # Convert to lowercase
    cleaned = cleaned.lower()
    # Normalize multiple whitespaces
    cleaned = EXTRA_WHITESPACE_REGEX.sub(" ", cleaned).strip()
    return cleaned


def batch_clean_texts(texts: List[str]) -> List[str]:
    """
    Applies clean_text across a list of text inputs.
    
    Args:
        texts: List of raw string reviews.
        
    Returns:
        List of cleaned strings.
    """
    return [clean_text(t) for t in texts]
