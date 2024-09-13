import requests
import xml.etree.ElementTree as ET
import re
import json
import boto3
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Define the namespace
ns = {'ofac': 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML'}

# Define the digital currency address types we're interested in
DIGITAL_CURRENCY_TYPES = [
    "Digital Currency Address - ETH",
    "Digital Currency Address - XBT",
    "Digital Currency Address - TRX",
    "Digital Currency Address - USDT",
    "Digital Currency Address - USDC"
]

def download_ofac_data(url="https://www.treasury.gov/ofac/downloads/sdn.xml"):
    try:
        response = requests.get(url)
        response.raise_for_status()
        content_type = response.headers.get('Content-Type', '')
        print(f"Downloaded {len(response.content)} bytes of data")
        print(f"Content-Type: {content_type}")
        return response.content
    except requests.RequestException as e:
        print(f"Error downloading data: {e}")
        return None

def parse_xml_data(xml_file):
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        entries = root.findall(".//ofac:sdnEntry", namespaces=ns)
        print(f"Number of sdnEntry elements found: {len(entries)}")
        return entries
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return []

def process_entry(entry):
    result = {
        "uid": entry.find("ofac:uid", namespaces=ns).text if entry.find("ofac:uid", namespaces=ns) is not None else "",
        "sdnType": entry.find("ofac:sdnType", namespaces=ns).text if entry.find("ofac:sdnType", namespaces=ns) is not None else "",
        "firstName": entry.find("ofac:firstName", namespaces=ns).text if entry.find("ofac:firstName", namespaces=ns) is not None else "",
        "lastName": entry.find("ofac:lastName", namespaces=ns).text if entry.find("ofac:lastName", namespaces=ns) is not None else "",
        "idList": [],
        "akaList": [],
        "addressList": []
    }
    
    # Process idList
    id_list = entry.find("ofac:idList", namespaces=ns)
    has_digital_currency = False
    if id_list is not None:
        for id_entry in id_list.findall("ofac:id", namespaces=ns):
            id_type = id_entry.find("ofac:idType", namespaces=ns).text if id_entry.find("ofac:idType", namespaces=ns) is not None else ""
            id_number = id_entry.find("ofac:idNumber", namespaces=ns).text if id_entry.find("ofac:idNumber", namespaces=ns) is not None else ""
            result["idList"].append({"idType": id_type, "idNumber": id_number})
            if id_type in DIGITAL_CURRENCY_TYPES:
                has_digital_currency = True
    
    if not has_digital_currency:
        return None
    
    # Process akaList
    aka_list = entry.find("ofac:akaList", namespaces=ns)
    if aka_list is not None:
        for aka in aka_list.findall("ofac:aka", namespaces=ns):
            aka_type = aka.find("ofac:type", namespaces=ns).text if aka.find("ofac:type", namespaces=ns) is not None else ""
            aka_name = aka.find("ofac:lastName", namespaces=ns).text if aka.find("ofac:lastName", namespaces=ns) is not None else ""
            if aka.find("ofac:firstName", namespaces=ns) is not None:
                aka_name = f"{aka.find('ofac:firstName', namespaces=ns).text} {aka_name}"
            result["akaList"].append({"type": aka_type, "name": aka_name.strip()})
    
    # Process addressList
    address_list = entry.find("ofac:addressList", namespaces=ns)
    if address_list is not None:
        for address in address_list.findall("ofac:address", namespaces=ns):
            address_data = {}
            for child in address:
                address_data[child.tag.split('}')[-1]] = child.text
            result["addressList"].append(address_data)
    
    return result

def save_to_file(data, filename="processed_ofac_data.json"):
    full_path = os.path.abspath(filename)
    with open(full_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Data saved to: {full_path}")

def upload_to_s3(filename, bucket_name, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(filename, bucket_name, s3_key)

def write_to_sheet(data, spreadsheet_id, range_name):
    # Set up credentials
    creds = Credentials.from_service_account_file(
        '/Users/jasoncomer/Desktop/explorer-master/OFAC_automation/ofac-automation-c10f419ade0d.json',
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )

    # Build the Sheets API service
    service = build('sheets', 'v4', credentials=creds)

    # Prepare the data for writing
    values = [['UID', 'SDN Type', 'First Name', 'Last Name', 'Digital Currency Address', 'Entity ID', 'Websites']]
    for entry in data:
        digital_currency_addresses = [id['idNumber'] for id in entry['idList'] if id['idType'] in DIGITAL_CURRENCY_TYPES]
        
        # Collect all websites
        websites = [id['idNumber'] for id in entry['idList'] if id['idType'] == 'Website']
        websites_str = ', '.join(websites)
        
        # Create entity_id based on SDN Type
        if entry['sdnType'] == 'Individual':
            entity_id = f"{entry['firstName']}_{entry['lastName']}".replace(' ', '_').lower()
        else:  # Assuming 'Entity' or any other type
            entity_id = entry['lastName'].replace(' ', '_').lower()
        
        # Replace periods with underscores and remove commas from entity_id
        entity_id = entity_id.replace('.', '_').replace(',', '')
        
        # Create a new row for each digital currency address
        for address in digital_currency_addresses:
            values.append([
                entry['uid'],
                entry['sdnType'],
                entry['firstName'],
                entry['lastName'],
                address,
                entity_id,
                websites_str
            ])

    # Write to the sheet
    body = {'values': values}
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption='RAW', body=body).execute()
    print(f"{result.get('updatedCells')} cells updated.")

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    print(f"Current working directory: {os.getcwd()}")

    xml_file = 'downloaded_file.xml'
    if os.path.exists(xml_file):
        print(f"XML file size: {os.path.getsize(xml_file)} bytes")
    else:
        print(f"Error: {xml_file} not found")
        return

    entries = parse_xml_data(xml_file)
    processed_data = [entry for entry in (process_entry(entry) for entry in entries) if entry is not None]
    
    spreadsheet_id = '1wht_3zc0dt5VwclbWq0KjEjaEAKGjRAblOFVe6pyfiE'
    range_name = 'ofac!A1'  # This will start writing from cell A1 in the 'ofac' tab
    
    write_to_sheet(processed_data, spreadsheet_id, range_name)
    
    print(f"Processed {len(processed_data)} entries with digital currency addresses. Data written to Google Sheet.")

if __name__ == "__main__":
    main()
