import re
import json
from typing import Any, Dict, List, Union

def clean_data(data: Union[List, Dict]) -> Union[List, Dict]:
    """
    Clean JSON data by removing extra spaces, trimming words, and normalizing text.
    
    Args:
        data: JSON data as list or dict
        
    Returns:
        Cleaned JSON data
    """
    
    def clean_string(text: str) -> str:
        """Clean individual string values"""
        if not isinstance(text, str):
            return text
            
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)
        
        # Replace non-breaking spaces and other special spaces
        text = text.replace('\xa0', ' ').replace('\u200b', '').replace('\u202f', ' ')
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        # Remove extra whitespace around punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        text = re.sub(r'([.,;:!?])\s+', r'\1 ', text)
        
        return text
    
    def clean_tags(tags: List[str]) -> List[str]:
        """Clean article tags list"""
        if not isinstance(tags, list):
            return tags
            
        cleaned_tags = []
        for tag in tags:
            if isinstance(tag, str):
                # Clean the tag string
                cleaned_tag = clean_string(tag)
                # Remove empty tags
                if cleaned_tag:
                    cleaned_tags.append(cleaned_tag)
        
        return cleaned_tags
    
    def clean_content(text: str) -> str:
        """Special cleaning for article content"""
        if not isinstance(text, str):
            return text
            
        text = clean_string(text)
        
        # Fix common content issues
        text = re.sub(r'\s*—\s*', ' — ', text)  # Proper em dash spacing
        text = re.sub(r'\s*-\s*', ' - ', text)  # Proper hyphen spacing
        text = re.sub(r'\s*\.\s*', '. ', text)  # Proper period spacing
        
        # Ensure proper paragraph breaks
        text = re.sub(r'\n\s*\n', '\n\n', text)
        
        return text
    
    def clean_item(item: Dict[str, Any]) -> Dict[str, Any]:
        """Clean individual dictionary item"""
        if not isinstance(item, dict):
            return item
            
        cleaned_item = {}
        
        for key, value in item.items():
            if value is None:
                cleaned_item[key] = None
            elif isinstance(value, str):
                if key == 'article_content':
                    cleaned_item[key] = clean_content(value)
                elif key == 'article_title':
                    cleaned_item[key] = clean_string(value)
                elif key == 'article_description':
                    cleaned_item[key] = clean_string(value) if value else None
                else:
                    cleaned_item[key] = clean_string(value)
            elif isinstance(value, list) and key == 'article_tags':
                cleaned_item[key] = clean_tags(value)
            elif isinstance(value, list):
                cleaned_item[key] = [clean_string(v) if isinstance(v, str) else v for v in value]
            else:
                cleaned_item[key] = value
        
        return cleaned_item
    
    # Main cleaning logic
    if isinstance(data, list):
        return [clean_item(item) for item in data]
    elif isinstance(data, dict):
        return clean_item(data)
    else:
        return data


# Enhanced version with additional features
def clean_data_advanced(data: Union[List, Dict], 
                       remove_empty_tags: bool = True,
                       normalize_whitespace: bool = True,
                       fix_punctuation: bool = True) -> Union[List, Dict]:
    """
    Advanced data cleaning with configurable options.
    
    Args:
        data: JSON data to clean
        remove_empty_tags: Remove empty tags from article_tags
        normalize_whitespace: Normalize all whitespace characters
        fix_punctuation: Fix spacing around punctuation
        
    Returns:
        Cleaned JSON data
    """
    
    def advanced_clean_string(text: str) -> str:
        """Advanced string cleaning"""
        if not isinstance(text, str):
            return text
            
        # Normalize various space characters to regular spaces
        if normalize_whitespace:
            text = re.sub(r'[\xa0\u200b\u202f\u00a0]', ' ', text)
        
        # Collapse multiple spaces, tabs, newlines to single space
        text = re.sub(r'\s+', ' ', text)
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        # Fix punctuation spacing
        if fix_punctuation:
            # Fix spacing before punctuation
            text = re.sub(r'\s+([.,;:!?])', r'\1', text)
            # Fix spacing after punctuation (except when followed by quote or parenthesis)
            text = re.sub(r'([.,;:!?])([^\s)"\'])', r'\1 \2', text)
            # Fix spacing for em dashes and hyphens
            text = re.sub(r'\s*—\s*', ' — ', text)
            text = re.sub(r'\s*-\s*', ' - ', text)
        
        return text
    
    def clean_item_advanced(item: Dict[str, Any]) -> Dict[str, Any]:
        """Advanced item cleaning"""
        if not isinstance(item, dict):
            return item
            
        cleaned = {}
        
        for key, value in item.items():
            if value is None:
                cleaned[key] = None
            elif isinstance(value, str):
                if key == 'article_content':
                    # Special handling for content with paragraph preservation
                    content = advanced_clean_string(value)
                    # Preserve meaningful paragraph breaks
                    content = re.sub(r'\n\s*\n', '\n\n', content)
                    cleaned[key] = content
                else:
                    cleaned[key] = advanced_clean_string(value)
            elif isinstance(value, list) and key == 'article_tags':
                cleaned_tags = []
                for tag in value:
                    if isinstance(tag, str):
                        clean_tag = advanced_clean_string(tag)
                        if clean_tag and (not remove_empty_tags or clean_tag.strip()):
                            cleaned_tags.append(clean_tag)
                cleaned[key] = cleaned_tags
            elif isinstance(value, list):
                cleaned[key] = [advanced_clean_string(v) if isinstance(v, str) else v for v in value]
            else:
                cleaned[key] = value
        
        return cleaned
    
    # Apply cleaning
    if isinstance(data, list):
        return [clean_item_advanced(item) for item in data]
    elif isinstance(data, dict):
        return clean_item_advanced(data)
    else:
        return data
if __name__ == "__main__":
    pass