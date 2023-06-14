from pyairtable import Api, Base, Table
from decouple import config
import requests
import json
from bs4 import BeautifulSoup

# get airtable key from .env file
airtable_api_key = config("airtable_api_key")

# using this headers in beautifulsoup to scrape the page
headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "3600",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0",
}

# BASE_ID is not sensitive info, we can leave it as a constant
BASE_ID = "appIqtI7VuPJnQZ7y"  # smoke alerts


### TABLE CONSTANTS
INCIDENTS_TABLE = "Emergency WA - Incidents"
AIRQ_TABLE = "Air Quality Index"
LOCAL_GOV_TABLE = "Emergency WA - Local Government"
SUBDISTRICTS_TABLE = "Emergency WA - Subdistricts"
WARNINGS_TABLE = "Emergency WA - Warnings"


# functions to get records from a table
def get_airtable_records(table, view="Grid view"):
    print("Getting records from " + table)
    records = Table(airtable_api_key, BASE_ID, table).all()
    print(records)
    return records


def create_record(table, record):
    table = Table(airtable_api_key, BASE_ID, table)
    rec = table.create(record)
    id = rec["id"]
    return id


def batch_create_records(table, records):
    table = Table(airtable_api_key, BASE_ID, table)
    rec = table.batch_create(records)
    return len(rec)


def update_record(table, id, record):
    table = Table(airtable_api_key, BASE_ID, table)
    rec = table.update(id, record)
    id = rec["id"]
    return id


# function to find existing record by incident id of
def find_record_id_by_id(existing_records, id):
    ex_record_id = [x for x in existing_records if x["fields"]["id"] == id]
    return ex_record_id[0]["id"]


# function to find existing record by location name, used for air quality table
def find_record_id_by_location(existing_records, location):
    ex_record_id = [x for x in existing_records if x["fields"]["location"] == location]
    print(ex_record_id)
    if ex_record_id:
        return ex_record_id[0]["id"]


# function to get JSON from source
def get_target_json(target):
    resp = requests.get(target).json()
    return resp


# function to scrape table with beautiful soup
def get_target_table(target):
    req = requests.get(target, headers)
    soup = BeautifulSoup(req.content, "html.parser")
    # find all td class tr to process locations and statuses
    tables = soup.find_all("td", class_="tr")
    airq = []
    for table in tables:
        b = table.find_all("b")
        if len(b) > 1:
            location = b[0].get_text().replace(" A.Q.M.S.", "").strip()
            if "Nth " in location:
                location = location.replace("Nth ", "")
            status = b[1].get_text().strip()
            airq.append({"location": location, "status": status})
    return airq


# reduce lists of dictionaries from airtable tables to simple lists with existing ids
def reduce_list(airtable_records):
    existing_id = []
    for record in airtable_records:
        existing_id.append(record["fields"]["id"])
    return existing_id


# reduce lists of dictionaries from airtable tables to simple lists with existing locations
def reduce_list_to_locations(airtable_records):
    existing_locations = []
    for record in airtable_records:
        existing_locations.append(record["fields"]["location"])
    return existing_locations


# Get all records from all the tables in the base
# existing_emwa_local_gov = get_airtable_records(LOCAL_GOV_TABLE)
existing_emwa_subdistricts = get_airtable_records(SUBDISTRICTS_TABLE)
existing_emwa_incidents = get_airtable_records(INCIDENTS_TABLE)
existing_emwa_warnings = get_airtable_records(WARNINGS_TABLE)
exisitng_airq_records = get_airtable_records(AIRQ_TABLE)


# scrape sources to arrays
airq = get_target_table(
    "https://www.der.wa.gov.au/your-environment/air/air-quality-index"
)
incidents = get_target_json("https://www.emergency.wa.gov.au/data/incident_FCAD.json")[
    "features"
]
districts = get_target_json(
    "https://www.emergency.wa.gov.au/data/BOM_Fire_Weather_Subdistricts.json"
)
warnings = get_target_json(
    "https://www.emergency.wa.gov.au/data/message_warnings.json"
)["features"]

localgovs = get_target_json(
    "https://www.emergency.wa.gov.au/data/TFB_Local_Governments.json"
)["features"]


# Get list of ids to check
existing_incident_ids = reduce_list(existing_emwa_incidents)
existing_warning_ids = reduce_list(existing_emwa_warnings)
existing_subdistrict_id = reduce_list(existing_emwa_subdistricts)
# existing_gov_id = reduce_list(existing_emwa_local_gov)
existing_airq_locations = reduce_list_to_locations(exisitng_airq_records)


### go through newly scraped data and either update existing record or create new record ###
def process_incidents():
    for incident in incidents:
        id = int(incident["properties"]["incidentEventsId"])
        region = incident.get("properties").get("region").title()
        suburb = incident.get("properties").get("locationSuburb").title()
        localgov = incident.get("properties").get("localGovernmentArea").title()
        type = incident.get("properties").get("type")
        description = incident.get("properties").get("description")
        incidentid = int(incident.get("properties").get("incidentEventsId"))
        lattitude = str(incident.get("geometry").get("coordinates")[1])
        longitude = str(incident.get("geometry").get("coordinates")[0])
        lastupdate = incident.get("properties").get("lastUpdatedTime")
        new_record = {
            "region": region,
            "suburb": suburb,
            "localgov": localgov,
            "type": type,
            "description": description,
            "id": incidentid,
            "lattitude": lattitude,
            "longitude": longitude,
            "lastupdate": lastupdate,
        }
        # print(id)
        if id in existing_incident_ids:
            # print("exists")
            existing_id = find_record_id_by_id(existing_emwa_incidents, id)
            # print(existing_id)
            update_record(INCIDENTS_TABLE, existing_id, new_record)
        else:
            create_record(INCIDENTS_TABLE, new_record)


# are we getting objects? Then it makes sense to just include coordinates as they are.
def process_localgovs():
    records = []
    for gov in localgovs:
        geometry = gov["geometry"].get("coordinates")
        longname = gov.get("properties").get("LONG_NAME").title()
        name = gov.get("properties").get("NAME").title()
        id = int(gov.get("properties").get("OBJECTID"))
        if geometry is not None:
            for coords in geometry:
                for item in coords:
                    if len(item) > 1:
                        lattitude = str(item[1])
                        longitude = str(item[0])
                    coord_obj = {
                        "id": id,
                        "name": name,
                        "longname": longname,
                        "lattitude": lattitude,
                        "longitude": longitude,
                    }
                    records.append(coord_obj)
    # batch_create_records(LOCAL_GOV_TABLE, records)


# warnings
def process_warnings():
    for warning in warnings:
        suburb = warning.get("properties").get("locationSuburb").title()
        subject = warning.get("properties").get("subject").title()
        type = warning.get("properties").get("type")
        # description = incident.get('properties').get('description')
        id = int(warning.get("properties").get("incidentEventsId"))
        lattitude = str(warning.get("geometry").get("coordinates")[1])
        longitude = str(warning.get("geometry").get("coordinates")[0])
        lastupdate = warning.get("properties").get("lastUpdatedTime")[:-3]
        new_record = {
            # "region": region,
            "suburb": suburb,
            "subject": subject,
            "type": type,
            # "description": description,
            "id": id,
            "lattitude": lattitude,
            "longitude": longitude,
            "lastupdate": lastupdate,
        }
        if id in existing_warning_ids:
            # print("exists")
            existing_id = find_record_id_by_id(existing_emwa_warnings, id)
            # print(existing_id)
            update_record(WARNINGS_TABLE, existing_id, new_record)
        else:
            create_record(WARNINGS_TABLE, new_record)


def process_airquality():
    for air in airq:
        if air["location"] in existing_airq_locations:
            id = find_record_id_by_location(exisitng_airq_records, air["location"])
            print(id)
            update_record(AIRQ_TABLE, id, air)
        else:
            create_record(AIRQ_TABLE, air)


### regular updates ###
print("updating air quality")
process_airquality()
print("updating incidents")
process_warnings()
print("updating warnings")
# process_incidents()
### update this for local gov geography coordinates ###
# process_localgovs()
