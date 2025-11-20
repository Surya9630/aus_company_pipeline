#!/usr/bin/env python3
"""
generate_sample_abr.py

Generate sample ABR XML data for testing without downloading full ABR dataset.
Creates realistic Australian company data in ABR XML format.
"""

import argparse
import random
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os

# Sample Australian company data
COMPANY_NAMES = [
    "Acme Pty Ltd", "Sydney Services", "Melbourne Tech", "Brisbane Consulting",
    "Perth Mining Co", "Adelaide Retail", "Canberra Solutions", "Darwin Logistics",
    "Gold Coast Hospitality", "Hobart Manufacturing", "Newcastle Construction",
    "Wollongong Trading", "Geelong Industries", "Townsville Exports",
    "Cairns Tourism", "Toowoomba Agriculture", "Ballarat Manufacturing"
]

ENTITY_TYPES = ["Company", "Partnership", "Trust", "Sole Trader"]
STATES = ["NSW", "VIC", "QLD", "WA", "SA", "ACT", "NT", "TAS"]
STREETS = ["Main St", "High St", "George St", "Elizabeth St", "King St", "Queen St"]

def generate_abn():
    """Generate a random 11-digit ABN."""
    return ''.join([str(random.randint(0, 9)) for _ in range(11)])

def generate_address(state):
    """Generate a realistic Australian address."""
    street_num = random.randint(1, 999)
    street = random.choice(STREETS)
    postcode = {
        "NSW": random.randint(2000, 2999),
        "VIC": random.randint(3000, 3999),
        "QLD": random.randint(4000, 4999),
        "SA": random.randint(5000, 5999),
        "WA": random.randint(6000, 6999),
        "TAS": random.randint(7000, 7999),
        "ACT": random.randint(2600, 2699),
        "NT": random.randint(800, 899),
    }.get(state, 2000)
    
    return {
        "line1": f"{street_num} {street}",
        "line2": "",
        "postcode": str(postcode),
        "state": state
    }

def generate_sample_data(count=100):
    """Generate sample ABR records."""
    records = []
    
    for i in range(count):
        abn = generate_abn()
        company_name = random.choice(COMPANY_NAMES)
        if i < len(COMPANY_NAMES):
            company_name = COMPANY_NAMES[i] + " " + str(random.randint(1, 100))
        
        entity_type = random.choice(ENTITY_TYPES)
        state = random.choice(STATES)
        address = generate_address(state)
        
        # Random start date within last 20 years
        days_ago = random.randint(0, 20 * 365)
        start_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        
        status = "Active" if random.random() > 0.2 else "Cancelled"
        
        records.append({
            "abn": abn,
            "name": company_name,
            "entity_type": entity_type,
            "status": status,
            "address": address,
            "start_date": start_date
        })
    
    return records

def create_abr_xml(records, output_path):
    """Create ABR XML file from records."""
    root = ET.Element("ABRFile")
    
    for record in records:
        abr_elem = ET.SubElement(root, "ABR")
        
        # ABN element with attributes
        abn_elem = ET.SubElement(abr_elem, "ABN")
        abn_elem.text = record["abn"]
        abn_elem.set("status", record["status"])
        abn_elem.set("ABNStatus", record["status"])
        abn_elem.set("ABNStatusFromDate", record["start_date"])
        
        # Entity type
        entity_type_elem = ET.SubElement(abr_elem, "EntityType")
        entity_type_ind = ET.SubElement(entity_type_elem, "EntityTypeInd")
        entity_type_ind.text = record["entity_type"]
        
        # Main entity
        main_entity = ET.SubElement(abr_elem, "MainEntity")
        
        # Non-individual name
        non_ind_name = ET.SubElement(main_entity, "NonIndividualName")
        non_ind_name_text = ET.SubElement(non_ind_name, "NonIndividualNameText")
        non_ind_name_text.text = record["name"]
        
        # Business address
        business_addr = ET.SubElement(main_entity, "BusinessAddress")
        addr_details = ET.SubElement(business_addr, "AddressDetails")
        
        addr_line1 = ET.SubElement(addr_details, "AddressLine1")
        addr_line1.text = record["address"]["line1"]
        
        if record["address"]["line2"]:
            addr_line2 = ET.SubElement(addr_details, "AddressLine2")
            addr_line2.text = record["address"]["line2"]
        
        state_elem = ET.SubElement(addr_details, "State")
        state_elem.text = record["address"]["state"]
        
        postcode_elem = ET.SubElement(addr_details, "Postcode")
        postcode_elem.text = record["address"]["postcode"]
    
    # Create pretty XML
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write to file
    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    print(f"Generated {len(records)} sample ABR records to {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Generate sample ABR XML data for testing")
    parser.add_argument("--count", type=int, default=100, help="Number of records to generate")
    parser.add_argument("--output", default="abr_data/sample_abr.xml", help="Output XML file path")
    
    args = parser.parse_args()
    
    records = generate_sample_data(args.count)
    create_abr_xml(records, args.output)

if __name__ == "__main__":
    main()
