#!/usr/bin/env python3
# scripts/create_sample_warc.py
from warcio.warcwriter import WARCWriter
from io import BytesIO
import gzip
import os

os.makedirs("samples", exist_ok=True)
out_path = "samples/sample_cc.warc.gz"

# Simple HTML pages
pages = [
    ("http://acme.com.au/", b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><head><title>Acme Pty Ltd</title><meta name='description' content='Acme services'/></head><body><h1>Acme Pty Ltd</h1><p>Acme is an Australian company</p></body></html>"),
    ("http://example.org/", b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><head><title>Example Org</title></head><body><h1>Example</h1><p>Not AU</p></body></html>")
]

with gzip.open(out_path, 'wb') as gz:
    writer = WARCWriter(gz, gzip=True)
    for url, http_payload in pages:
        # warcio expects a file-like for http payload
        payload_stream = BytesIO(http_payload)
        rec = writer.create_warc_record(url, 'response', payload=payload_stream, http_headers=None)
        writer.write_record(rec)

print(f"Created sample WARC at: {out_path}")
