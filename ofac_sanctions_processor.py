import requests
import xml.etree.ElementTree as ET
import re
import json
import boto3
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime
from googleapiclient.errors import HttpError

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
        "addressList": [],
        "websites": []
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
            elif id_type == "Website":
                result["websites"].append(id_number)
    
    if not has_digital_currency:
        return None
    
    # Create entity_id
    if result['sdnType'] == 'Individual':
        result['entity_id'] = f"{result['firstName']}_{result['lastName']}".replace(' ', '_').lower()
    else:
        result['entity_id'] = result['lastName'].replace(' ', '_').lower()
    
    # Remove periods and commas from entity_id
    result['entity_id'] = result['entity_id'].replace('.', '_').replace(',', '')
    
    return result

def save_to_file(data, filename="processed_ofac_data.json"):
    full_path = os.path.abspath(filename)
    with open(full_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Data saved to: {full_path}")

def upload_to_s3(filename, bucket_name, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(filename, bucket_name, s3_key)

def read_existing_entity_ids(service, spreadsheet_id):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='SoT!C:C'
        ).execute()
        values = result.get('values', [])
        return set(value[0] for value in values if value)
    except HttpError as error:
        print(f"An error occurred: {error}")
        return set()

def append_new_entity_ids(service, entities, existing_entity_ids):
    new_spreadsheet_id = '1L87aWWnokU84mz_VbALqnTsqu1RhqJoHjWQnO-dVvJw'
    new_values = []
    current_date = datetime.now().strftime('%Y-%m-%d')

    for entity in entities:
        if entity['entity_id'] not in existing_entity_ids:
            website = entity['websites'][0] if entity['websites'] else ''
            display_name = f"{entity['firstName']} {entity['lastName']}".strip()
            new_values.append([
                current_date,  # Column A: Current date
                'ofac_automation',  # Column B: Source
                entity['entity_id'],  # Column C: entity_id
                website,  # Column D: First website
                display_name,  # Column E: Display name
                'ofac sanctioned'  # Column F: Status
            ])

    if new_values:
        try:
            # First, get the current number of rows in the sheet
            result = service.spreadsheets().values().get(
                spreadsheetId=new_spreadsheet_id,
                range='SoT!A:A'
            ).execute()
            last_row = len(result.get('values', []))

            # Now append the new values after the last row
            range_to_update = f'SoT!A{last_row + 1}:F{last_row + len(new_values)}'
            body = {'values': new_values}
            service.spreadsheets().values().update(
                spreadsheetId=new_spreadsheet_id,
                range=range_to_update,
                valueInputOption='RAW',
                body=body
            ).execute()
            print(f"Appended {len(new_values)} new entity_ids to the SoT spreadsheet after row {last_row}.")
        except HttpError as error:
            print(f"An error occurred while appending: {error}")
    else:
        print("No new entity_ids to append to the SoT spreadsheet.")

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
    entities_to_append = []  # List to store entities for appending to the new sheet

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
        
        # Store entity information for appending to the new sheet
        entities_to_append.append({
            'entity_id': entity_id,
            'firstName': entry['firstName'],
            'lastName': entry['lastName'],
            'websites': websites
        })

    # Write to the sheet
    body = {'values': values}
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption='RAW', body=body).execute()
    print(f"{result.get('updatedCells')} cells updated.")

    # After writing to the original sheet, handle the SoT sheet
    sot_spreadsheet_id = '1L87aWWnokU84mz_VbALqnTsqu1RhqJoHjWQnO-dVvJw'
    try:
        existing_entity_ids = read_existing_entity_ids(service, sot_spreadsheet_id)
        append_new_entity_ids(service, entities_to_append, existing_entity_ids)
    except HttpError as error:
        print(f"An error occurred while handling the SoT sheet: {error}")

def test_spreadsheet_access(spreadsheet_id):
    creds = Credentials.from_service_account_file(
        '/Users/jasoncomer/Desktop/explorer-master/OFAC_automation/ofac-automation-c10f419ade0d.json',
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    service = build('sheets', 'v4', credentials=creds)
    
    try:
        # Try to get the spreadsheet metadata
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        print(f"Successfully accessed spreadsheet: {sheet_metadata['properties']['title']}")
        
        # Try to read from the 'SoT' sheet
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='SoT!A1:A2'
        ).execute()
        print(f"Successfully read from 'SoT' sheet. Values: {result.get('values', [])}")
        
    except HttpError as error:
        print(f"An error occurred: {error}")
        if error.resp.status == 404:
            print("Spreadsheet not found. Please check the ID and permissions.")
        elif error.resp.status == 403:
            print("Permission denied. Please check that the service account has access to the spreadsheet.")

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
    
    spreadsheet_id = '1wht_3zc0dt5VwclbWq0KjEjaEAKGjRAblOFVe6pyfiE'  # ofac_automation sheet
    range_name = 'ofac!A1'  # This will start writing from cell A1 in the 'ofac' tab
    
    write_to_sheet(processed_data, spreadsheet_id, range_name)
    
    print(f"Processed {len(processed_data)} entries with digital currency addresses. Data written to Google Sheet.")
    
    test_spreadsheet_access('1L87aWWnokU84mz_VbALqnTsqu1RhqJoHjWQnO-dVvJw')

if __name__ == "__main__":
    main()
