#!/usr/bin/env python3
"""
cc_extractor.py

Processes Common Crawl WARC and WET files to extract Australian company data.

Supports:
- WARC files (.warc.gz) - Full HTTP response with HTML
- WET files (.warc.wet.gz) - Plain text extracted content

Usage (fast, no-LLM):
  python src/extraction/common_crawl_extractor.py \
    --input samples/sample_cc.warc.gz \
    --output outputs/cc_extracted.jsonl \
    --max 1000 --no-llm

Usage (WET file with batch processing):
  python src/extraction/common_crawl_extractor.py \
    --input-dir cc_data/crawl-data/CC-MAIN-2025-13/segments/*/wet/ \
    --output outputs/cc_extracted.jsonl \
    --max 100000 --no-llm

Usage (with LLM fallback - requires GEMINI_API_KEY in .env):
  python src/extraction/common_crawl_extractor.py \
    --input samples/sample_cc.warc.gz \
    --output outputs/cc_extracted.jsonl \
    --max 500 --use-llm
"""
import argparse
import gzip
import json
import re
import os
import sys
import time
import glob
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import urlparse
from tqdm import tqdm
from dotenv import load_dotenv

# ABN extraction patterns (multiple formats)
ABN_PATTERNS = [
    r'ABN[:\s]+(\d{2}\s?\d{3}\s?\d{3}\s?\d{3})',  # ABN: 12 345 678 901
    r'ABN[:\s]+(\d{11})',  # ABN: 12345678901
    r'Australian Business Number[:\s]+(\d{2}\s?\d{3}\s?\d{3}\s?\d{3})',
    r'Australian Business Number[:\s]+(\d{11})',
    r'A\.?B\.?N\.?[:\s]+(\d{2}\s?\d{3}\s?\d{3}\s?\d{3})',
    r'A\.?B\.?N\.?[:\s]+(\d{11})',
]

# Load environment
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Optional LLM import (wrapped so missing package won't crash script)
genai = None
if GEMINI_API_KEY:
    try:
        import google.generativeai as genai
    except Exception:
        genai = None

# If genai present and key present, configure
if genai and GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
    except Exception:
        # ignore configure errors; we'll handle at call time
        pass

# --- helpers ---
def canonical_domain(url):
    try:
        p = urlparse(url)
        host = p.netloc.lower()
    except Exception:
        return ""
    host = re.sub(r"^www\.", "", host)
    return host.strip(".")

def is_australian_domain(domain):
    if not domain:
        return False
    return (
        domain.endswith(".au")
        or ".com.au" in domain
        or ".gov.au" in domain
        or ".org.au" in domain
        or ".edu.au" in domain
    )

def normalize_abn(abn_str):
    """
    Normalize ABN to 11 digits without spaces.
    Input: "12 345 678 901" or "12345678901"
    Output: "12345678901"
    """
    if not abn_str:
        return None
    # Remove all whitespace and non-digits
    digits = re.sub(r'\D', '', abn_str)
    # Validate length
    if len(digits) == 11:
        return digits
    return None

def extract_abn_from_text(text):
    """
    Extract ABN from text content using multiple patterns.
    Returns normalized ABN (11 digits) or None.
    """
    if not text:
        return None
    
    for pattern in ABN_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            abn = match.group(1)
            normalized = normalize_abn(abn)
            if normalized:
                return normalized
    return None


def extract_html_fields_fast(html):
    """Extract structured data from HTML including ABN if present."""
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    meta_desc = ""
    tag = soup.find("meta", attrs={"name": "description"})
    if tag and tag.get("content"):
        meta_desc = tag["content"].strip()
    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True) if h1 else ""
    org_name = ""
    industry = ""
    
    # Extract ABN from HTML text
    html_text = soup.get_text()
    scraped_abn = extract_abn_from_text(html_text)
    
    jsonld_tags = soup.find_all("script", type="application/ld+json")
    for tag in jsonld_tags:
        try:
            data = json.loads(tag.string)
            if isinstance(data, dict):
                graph = data.get('@graph', [])
                if not isinstance(graph, list):
                    graph = [graph]
                all_nodes = [data] + graph
                for node in all_nodes:
                    if not isinstance(node, dict):
                        continue
                    node_type = node.get("@type")
                    if isinstance(node_type, list):
                        node_type = node_type[0]
                    if node_type in ["Organization", "LocalBusiness", "Corporation", "WebSite"]:
                        org_name = node.get("name", org_name)
                        industry = node.get("industry", industry)
                    # sometimes name under publisher
                    publisher = node.get("publisher")
                    if isinstance(publisher, dict) and not org_name:
                        if publisher.get("@type") in ["Organization", "LocalBusiness", "Corporation"]:
                            org_name = publisher.get("name", org_name)
        except Exception:
            continue
    if not org_name:
        org_name = h1_text or title
    return {
        "title": title,
        "meta_description": meta_desc,
        "h1": h1_text,
        "schema_org_name": str(org_name).strip(),
        "industry": str(industry).strip(),
        "scraped_abn": scraped_abn,
    }


def extract_details_with_llm(html_content, url, max_chars=15000):
    """Return (company_name, industry) or (None, None). Safely wrapped."""
    if not genai or not GEMINI_API_KEY:
        return None, None
    try:
        soup = BeautifulSoup(html_content, "lxml")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        page_text = ' '.join(soup.stripped_strings)
        if len(page_text) > max_chars:
            page_text = page_text[:max_chars] + "..."
    except Exception:
        return None, None

    prompt = (
        "You will receive website text. Output ONLY a JSON object with keys: "
        '"company_name" and "industry". If not a company page, set both to null.\n\n'
        f"URL: {url}\n\nTEXT:\n{page_text}\n\nJSON:"
    )

    # Attempt to call Gemini (best-effort)
    try:
        # If the package exposes a simple generate API, try common patterns.
        # We handle variations and parse the text as JSON.
        for attempt in range(3):
            try:
                # many genai versions expose `generate_text` or `chat.completions.create`.
                if hasattr(genai, "generate_text"):
                    resp = genai.generate_text(model="models/text-bison-001", prompt=prompt)
                    txt = getattr(resp, "text", None) or resp
                    txt = txt.strip()
                elif hasattr(genai, "text"):
                    # fallback
                    resp = genai.text.generate(model="models/text-bison-001", prompt=prompt)
                    txt = resp.text
                elif hasattr(genai, "chat"):
                    # some versions use chat
                    resp = genai.chat.completions.create(model="gemini-1.5", messages=[{"role":"user","content":prompt}])
                    txt = resp.choices[0].message.content
                else:
                    # Unknown API surface - give up
                    return None, None

                # strip markdown fences if present
                txt = txt.lstrip("```json").rstrip("```").strip()
                # find first JSON object
                m = re.search(r'(\{.*\})', txt, flags=re.S)
                json_text = m.group(1) if m else txt
                data = json.loads(json_text)
                return data.get("company_name"), data.get("industry")
            except Exception as e:
                # wait and retry
                time.sleep(2 + attempt * 3)
        return None, None
    except Exception:
        return None, None

def extract_common_crawl(input_path, output_path, max_records=None, use_llm=False):
    count = 0
    llm_calls = 0
    open_fn = gzip.open if input_path.endswith(".gz") else open

    try:
        with open_fn(input_path, "rb") as stream, open(output_path, "w", encoding="utf8") as out:
            for record in tqdm(ArchiveIterator(stream), desc="Processing WARC/WET", unit="rec"):
                if record.rec_type != "response" and record.rec_type != "conversion":
                    continue
                url = record.rec_headers.get_header("WARC-Target-URI")
                if not url:
                    continue
                domain = canonical_domain(url)
                if not is_australian_domain(domain):
                    continue
                try:
                    payload = record.content_stream().read()
                    html = payload.decode("utf-8", errors="ignore")
                except Exception:
                    continue
                fields = extract_html_fields_fast(html)
                company_name = fields.get("schema_org_name") or None
                industry = fields.get("industry") or None
                scraped_abn = fields.get("scraped_abn") or None
                source = "Schema"
                if not company_name and use_llm:
                    tqdm.write(f"[LLM] Falling back for {url}")
                    llm_name, llm_ind = extract_details_with_llm(html, url)
                    llm_calls += 1
                    if llm_name:
                        company_name = llm_name
                        source = "LLM"
                    if llm_ind:
                        industry = llm_ind
                row = {
                    "domain": domain,
                    "url": url,
                    "company_name": company_name,
                    "industry": industry,
                    "scraped_abn": scraped_abn,
                    "title": fields.get("title"),
                    "meta_description": fields.get("meta_description"),
                    "h1": fields.get("h1"),
                    "schema_org_name": fields.get("schema_org_name"),
                    "source": source
                }
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
                count += 1
                if max_records and count >= max_records:
                    tqdm.write(f"Reached max_records={max_records}. Stopping.")
                    break
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_path}", file=sys.stderr)
        return
    print(f"\n--- Extraction Complete ---")
    print(f"Processed {count} Australian pages.")
    print(f"Used LLM fallback {llm_calls} times.")
    print(f"Output saved to: {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Common Crawl Hybrid Extractor")
    parser.add_argument("-i", "--input", dest="input_path", help="Path to a single input WARC/WET.gz file.")
    parser.add_argument("-d", "--input-dir", dest="input_dir", help="Directory containing multiple WARC/WET.gz files.")
    parser.add_argument("-o", "--output", dest="output_path", required=True, help="Path to the output .jsonl file.")
    parser.add_argument("-m", "--max", dest="max_records", type=int, default=None,
                        help="Maximum number of records to process (for testing).")
    parser.add_argument("--use-llm", dest="use_llm", action="store_true",
                        help="Enable LLM fallback (requires GEMINI_API_KEY in .env).")
    parser.add_argument("--no-llm", dest="no_llm", action="store_true",
                        help="Disable LLM fallback (overrides --use-llm).")
    args = parser.parse_args()

    # Validate arguments
    if not args.input_path and not args.input_dir:
        print("Error: You must specify either --input or --input-dir")
        sys.exit(1)

    use_llm = args.use_llm and not args.no_llm
    if use_llm and not GEMINI_API_KEY:
        print("Warning: GEMINI_API_KEY not found in .env. Running with LLM disabled.", file=sys.stderr)
        use_llm = False

    print("Starting pipeline...")
    print(f"  Output File: {args.output_path}")
    if args.max_records:
        print(f"  Max Records: {args.max_records}")
    print(f"  LLM enabled: {use_llm}")

    # Process files
    if args.input_path:
        # Single file mode
        print(f"  Input File: {args.input_path}")
        extract_common_crawl(args.input_path, args.output_path, args.max_records, use_llm=use_llm)
    else:
        # Batch directory mode
        print(f"  Input Directory: {args.input_dir}")
        pattern = os.path.join(args.input_dir, "**/*.gz")
        files = glob.glob(pattern, recursive=True)
        print(f"  Found {len(files)} .gz files")
        
        if not files:
            print("Error: No .gz files found in input directory")
            sys.exit(1)
        
        # Process each file, appending to output
        total_count = 0
        for idx, file_path in enumerate(files, 1):
            print(f"\n[{idx}/{len(files)}] Processing: {os.path.basename(file_path)}")
            # For batch mode, we append to the same output file
            mode = "a" if idx > 1 else "w"
            remaining_records = None
            if args.max_records:
                remaining_records = args.max_records - total_count
                if remaining_records <= 0:
                    print("Reached maximum records across all files. Stopping.")
                    break
            
            # Process this file
            extract_common_crawl(file_path, args.output_path, remaining_records, use_llm=use_llm)
            
            # Update count (rough estimate)
            if os.path.exists(args.output_path):
                with open(args.output_path, 'r') as f:
                    total_count = sum(1 for _ in f)

if __name__ == "__main__":
    main()
