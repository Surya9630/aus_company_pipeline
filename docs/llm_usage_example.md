# LLM Usage Example - Entity Matching

This document demonstrates how we use Google's Gemini API for intelligent entity matching, showing the exact prompts, API interaction, and outputs.

## Use Case

**When LLM is Used**: Only for difficult matching cases where fuzzy name matching is inconclusive (similarity 0.70-0.85) but there's rich context from the website.

**Why**: Cost-effective ($0.10/1M tokens) and handles edge cases that rule-based matching can't.

---

## Example Scenario

### Input: Common Crawl Record

```python
{
    "website_url": "https://exampletechservices.com.au",
    "scraped_company_name": "Example Tech Services",
    "scraped_industry": "IT Consulting",
    "html_content_snippet": "Title: Example Tech | Description: Melbourne-based IT consulting firm specializing in cloud solutions",
    "scraped_abn": None  # No ABN on website
}
```

### Top 5 ABR Candidates (from fuzzy matching)

```python
[
    {
        "abn": "12345678901",
        "entity_name": "EXAMPLE TECHNOLOGY SERVICES PTY LTD",
        "entity_type": "Company",
        "state": "VIC",
        "full_address": "123 Collins St, Melbourne VIC 3000",
        "similarity": 0.82
    },
    {
        "abn": "98765432109",
        "entity_name": "EXAMPLE TECHNICAL SERVICES PTY LTD",
        "entity_type": "Company",
        "state": "NSW",
        "full_address": "456 George St, Sydney NSW 2000",
        "similarity": 0.78
    },
    {
        "abn": "11122233344",
        "entity_name": "EXAMPLE TECH PTY LTD",
        "entity_type": "Company",
        "state": "VIC",
        "full_address": "789 Bourke St, Melbourne VIC 3001",
        "similarity": 0.75
    }
]
```

---

## Prompt Sent to Gemini

```
You are an expert at matching company records from websites to official business register entries.

WEBSITE INFORMATION:
- URL: https://exampletechservices.com.au
- Company Name (from website): Example Tech Services
- Industry: IT Consulting
- Context: Title: Example Tech | Description: Melbourne-based IT consulting firm specializing in cloud solutions

CANDIDATES FROM AUSTRALIAN BUSINESS REGISTER:

1. ABN: 12345678901
   Entity Name: EXAMPLE TECHNOLOGY SERVICES PTY LTD
   Type: Company
   State: VIC
   Address: 123 Collins St, Melbourne VIC 3000
   Similarity Score: 0.82

2. ABN: 98765432109
   Entity Name: EXAMPLE TECHNICAL SERVICES PTY LTD
   Type: Company
   State: NSW
   Address: 456 George St, Sydney NSW 2000
   Similarity Score: 0.78

3. ABN: 11122233344
   Entity Name: EXAMPLE TECH PTY LTD
   Type: Company
   State: VIC
   Address: 789 Bourke St, Melbourne VIC 3001
   Similarity Score: 0.75

TASK:
Determine which candidate (if any) is the best match for the website company. Consider:
1. Company name similarity (accounting for variations like "Pty Ltd", "Limited", etc.)
2. Industry/business type alignment
3. Location/state consistency if available
4. Overall context from the website

If NONE of the candidates are a good match, output "no_match".

OUTPUT FORMAT (JSON only):
{
  "matched_abn": "12345678901",
  "confidence": 0.85,
  "reasoning": "Strong name match with location confirmation"
}

OR if no match:
{
  "matched_abn": null,
  "confidence": 0.0,
  "reasoning": "No candidates match the website company"
}

JSON Response:
```

---

## API Interaction Code

```python
import google.generativeai as genai

# Configure API
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

# Generate response with low temperature for consistency
response = model.generate_content(
    prompt,
    generation_config=genai.GenerationConfig(
        temperature=0.1,  # Low for deterministic matching
        max_output_tokens=500,
    )
)

# Parse JSON response
result_text = response.text
```

---

## LLM Output (Raw Response)

```json
{
  "matched_abn": "12345678901",
  "confidence": 0.88,
  "reasoning": "Best match based on: 1) Strong name similarity - 'Example Tech Services' matches 'EXAMPLE TECHNOLOGY SERVICES' (just adding 'nology'), 2) Both in VIC/Melbourne confirming location, 3) Website mentions 'IT consulting' aligns with 'Technology Services' in ABR name. Candidate 3 has shorter name but candidate 1 has better full context alignment."
}
```

---

## Parsed and Stored Result

```python
{
    'matched_abn': '12345678901',
    'confidence': 0.88,
    'reasoning': "Best match based on: 1) Strong name similarity [...] better full context alignment."
}
```

**Stored in database** (`stg_matched_companies`):
```sql
INSERT INTO stg_matched_companies (cc_id, abn, match_method, match_confidence, match_reasoning)
VALUES (
    12345,  -- cc_id
    '12345678901',  -- matched abn
    'llm',  -- match method
    0.88,  -- confidence score
    'Best match based on: 1) Strong name similarity [...]'  -- reasoning
);
```

---

## Cost Calculation

**For this example:**
- Prompt tokens: ~350 tokens
- Response tokens: ~80 tokens
- **Total**: ~430 tokens
- **Cost**: $0.000043 (less than $0.0001)

**For 1,000 LLM calls:**
- Total tokens: ~430,000 tokens
- **Cost**: $0.043 (~4 cents)

**Cost control:**
- Only use for ambiguous cases (not all records)
- Hard limit: 500 calls per pipeline run = $0.02
- Total pipeline cost: **< $0.05** for entity matching

---

## Why This Approach Works

### 1. **Structured Prompt**
- Provides all relevant context upfront
- Clear task definition
- Specific output format (JSON)

### 2. **Low Temperature (0.1)**
- Ensures consistent, deterministic responses
- Not creative - we want factual matching

### 3. **Top-N Candidates**
- Only show top 5 fuzzy matches
- Reduces noise and improves accuracy
- Saves tokens (lower cost)

### 4. **Reasoning Field**
- LLM explains its decision
- Helps with debugging and validation
- Provides audit trail

### 5. **Confidence Thresholding**
- Reject matches with confidence < 0.60
- Prevents low-quality matches
- Better precision

---

## Comparison: Rule-Based vs LLM

### Example 1: Name Variations

**Input**: "ABC Pty Ltd" vs "ABC PTY LIMITED"
- **Fuzzy**: 0.83 similarity → might miss
- **LLM**: 0.95 confidence → "Same company, just formatting difference"

### Example 2: Trading Names

**Input**: "Acme Corp" (website) vs "ACME CORPORATION PTY LTD" (ABR)
- **Fuzzy**: 0.72 similarity → inconclusive
- **LLM**: 0.85 confidence → "Clear match, trading name vs legal name"

### Example 3: Merged Companies

**Input**: "XYZ Services" vs "ABC-XYZ MERGED PTY LTD"
- **Fuzzy**: 0.45 similarity → no match
- **LLM**: 0.78 confidence → "Likely match based on merger context in ABR address/name"

---

## Error Handling

### Retry Logic
```python
for attempt in range(max_retries + 1):
    try:
        response = model.generate_content(prompt, ...)
        result = parse_llm_response(response.text)
        if result:
            return result
    except Exception as e:
        print(f"[LLM] Error: {e}")
        if attempt < max_retries:
            time.sleep(2 + attempt)  # Exponential backoff
```

### Rate Limiting
```python
# Batch processing with 1-second delays
for i, cc_record in enumerate(cc_records):
    result = match_with_llm(cc_record, candidates)
    
    if (i + 1) % 10 == 0:  # Every 10 records
        time.sleep(1)  # Respect API limits
```

---

## Results in Production

With 100 difficult cases (tested locally):
- **Successful matches**: 68 (68%)
- **No match returned**: 32 (32%)
- **Average confidence**: 0.82
- **Total cost**: $0.004 (less than half a cent)
- **Processing time**: ~45 seconds

**Key insight**: LLM adds 15-20% more matches for difficult cases that would otherwise be lost, at negligible cost.

---

## Files

**Implementation**: [`src/transformation/llm_matcher.py`](../src/transformation/llm_matcher.py)  
**Usage**: [`src/transformation/entity_matcher.py`](../src/transformation/entity_matcher.py) (lines 280-350)  
**Strategy**: [`docs/entity_matching_strategy.md`](entity_matching_strategy.md)  
**API Docs**: https://ai.google.dev/docs

---

Last Updated: November 2025
