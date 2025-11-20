#!/usr/bin/env python3
"""
llm_matcher.py

LLM-based entity matching for difficult cases using Gemini API.

This module uses Google's Gemini API to make intelligent matching decisions
when rule-based and fuzzy matching are insufficient.
"""

import os
import json
import time
from typing import Optional, Dict, List
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Optional: Import Gemini (gracefully handle missing package)
try:
    import google.generativeai as genai
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')  # Fast, cost-effective
    _HAVE_GEMINI = bool(GEMINI_API_KEY)
except ImportException:
    genai = None
    model = None
    _HAVE_GEMINI = False


def match_with_llm(cc_record, abr_candidates: List[Dict], max_retries=2) -> Optional[Dict]:
    """
    Use LLM to determine the best match between a CC record and ABR candidates.
    
    Args:
        cc_record: Database row from stg_common_crawl
        abr_candidates: List of top ABR candidates (dicts with abn, entity_name, etc.)
        max_retries: Number of retry attempts if LLM call fails
    
    Returns:
        Dict with keys: matched_abn, confidence, reasoning
        Or None if no good match or LLM unavailable
    """
    if not _HAVE_GEMINI:
        print("[LLM] Warning: Gemini API not configured. Skipping LLM matching.")
        return None
    
    if not abr_candidates:
        return None
    
    # Build structured prompt
    prompt = build_matching_prompt(cc_record, abr_candidates)
    
    # Call Gemini with retries
    for attempt in range(max_retries + 1):
        try:
            response = model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(
                    temperature=0.1,  # Low temperature for consistency
                    max_output_tokens=500,
                )
            )
            
            # Parse response
            result = parse_llm_response(response.text)
            
            if result:
                return result
            else:
                print(f"[LLM] Warning: Failed to parse LLM response on attempt {attempt + 1}")
                
        except Exception as e:
            print(f"[LLM] Error on attempt {attempt + 1}: {e}")
            if attempt < max_retries:
                time.sleep(2 + attempt)  # Exponential backoff
    
    return None


def build_matching_prompt(cc_record, abr_candidates: List[Dict]) -> str:
    """
    Build a structured prompt for the LLM to make a matching decision.
    """
    # Format CC record info
    cc_info = f"""
WEBSITE INFORMATION:
- URL: {cc_record.website_url}
- Company Name (from website): {cc_record.scraped_company_name or 'N/A'}
- Industry: {cc_record.scraped_industry or 'N/A'}
- Context: {cc_record.html_content_snippet or 'N/A'}
""".strip()
    
    # Format ABR candidates
    candidates_info = "\nCANDIDATES FROM AUSTRALIAN BUSINESS REGISTER:\n"
    for i, candidate in enumerate(abr_candidates[:5], 1):  # Top 5 only
        candidates_info += f"""
{i}. ABN: {candidate['abn']}
   Entity Name: {candidate['entity_name']}
   Type: {candidate.get('entity_type', 'N/A')}
   State: {candidate.get('state', 'N/A')}
   Address: {candidate.get('full_address', 'N/A')[:100]}
   Similarity Score: {candidate.get('similarity', 0):.2f}
""".strip() + "\n"
    
    # Full prompt
    prompt = f"""You are an expert at matching company records from websites to official business register entries.

{cc_info}

{candidates_info}

TASK:
Determine which candidate (if any) is the best match for the website company. Consider:
1. Company name similarity (accounting for variations like "Pty Ltd", "Limited", etc.)
2. Industry/business type alignment
3. Location/state consistency if available
4. Overall context from the website

If NONE of the candidates are a good match, output "no_match".

OUTPUT FORMAT (JSON only):
{{
  "matched_abn": "12345678901",
  "confidence": 0.85,
  "reasoning": "Strong name match with location confirmation"
}}

OR if no match:
{{
  "matched_abn": null,
  "confidence": 0.0,
  "reasoning": "No candidates match the website company"
}}

JSON Response:"""
    
    return prompt


def parse_llm_response(response_text: str) -> Optional[Dict]:
    """
    Parse LLM response and extract matching decision.
    Handles various response formats and markdown code blocks.
    """
    if not response_text:
        return None
    
    # Clean response
    text = response_text.strip()
    
    # Remove markdown code fences if present
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    
    # Try to find JSON object
    import re
    json_match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
    if json_match:
        try:
            data = json.loads(json_match.group(0))
            
            # Validate structure
            if 'matched_abn' in data and 'confidence' in data:
                # Normalize null/None/"no_match"
                if data['matched_abn'] in [None, "null", "no_match", ""]:
                    return None  # No match
                
                # Ensure confidence is float between 0 and 1
                confidence = float(data['confidence'])
                if confidence < 0.5:  # Too low confidence
                    return None
                
                return {
                    'matched_abn': str(data['matched_abn']).strip(),
                    'confidence': min(max(confidence, 0.0), 1.0),
                    'reasoning': data.get('reasoning', 'LLM determined match')
                }
        except (json.JSONDecodeError, ValueError) as e:
            print(f"[LLM] Error parsing JSON: {e}")
            return None
    
    return None


def batch_match_with_llm(cc_records: List, get_candidates_fn, batch_size=10):
    """
    Process multiple records with rate limiting.
    
    Args:
        cc_records: List of CC records to match
        get_candidates_fn: Function that returns ABR candidates for a CC record
        batch_size: Number of records to process before pausing
    
    Yields:
        Tuples of (cc_record, match_result)
    """
    for i, cc_record in enumerate(cc_records):
        candidates = get_candidates_fn(cc_record)
        result = match_with_llm(cc_record, candidates)
        
        yield cc_record, result
        
        # Rate limiting: pause after each batch
        if (i + 1) % batch_size == 0:
            time.sleep(1)  # Respect API rate limits


# Example usage for testing
if __name__ == "__main__":
    # Test data
    class MockCCRecord:
        website_url = "https://example.com.au"
        scraped_company_name = "Example Company Pty Ltd"
        scraped_industry = "Technology"
        html_content_snippet = "Title: Example Company | Description: Leading tech firm"
    
    mock_candidates = [
        {
            'abn': '12345678901',
            'entity_name': 'EXAMPLE COMPANY PTY LTD',
            'entity_type': 'Company',
            'state': 'VIC',
            'full_address': '123 Example St, Melbourne VIC 3000',
            'similarity': 0.92
        },
        {
            'abn': '98765432109',
            'entity_name': 'EXAMPLE BUSINESS PTY LTD',
            'entity_type': 'Company',
            'state': 'NSW',
            'full_address': '456 Business Rd, Sydney NSW 2000',
            'similarity': 0.78
        }
    ]
    
    print("Testing LLM Matcher...")
    result = match_with_llm(MockCCRecord(), mock_candidates)
    
    if result:
        print(f"Match found: ABN {result['matched_abn']}")
        print(f"Confidence: {result['confidence']:.2f}")
        print(f"Reasoning: {result['reasoning']}")
    else:
        print("No match found or LLM unavailable")
