import scrapy
import json
import requests
from datetime import datetime

class WarningSpider(scrapy.Spider):
    name = 'warning'
    allowed_domains = ['emergency.wa.gov.au']
    start_urls = ['https://www.emergency.wa.gov.au/data/message_warnings.json']

    def parse(self, response):
        airtable_base_id = 'appuetjbGSC7dJNHl'
        airtable_table_name = 'Emergency WA - Warnings'
        airtable_api_key = 'keyaeXOkK8tB7sjZ1'
        endpoint = f'https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_name}'
        headers = {
            "Authorization": f"Bearer {airtable_api_key}",
            "Content-Type": "application/json"
        }

        incidents = json.loads(response.body)['features']

        for incident in incidents:
            # region = incident.get('properties').get('region').title()
            suburb = incident.get('properties').get('locationSuburb').title()
            subject = incident.get('properties').get('subject').title()
            type = incident.get('properties').get('type')
            # description = incident.get('properties').get('description')
            incidentid = int(incident.get('properties').get('incidentEventsId'))
            lattitude = str(incident.get('geometry').get('coordinates')[1])
            longitude = str(incident.get('geometry').get('coordinates')[0])
            lastupdate = incident.get('properties').get('lastUpdatedTime')[:-3]
            # lastupdate = datetime.strptime(lastupdateraw, '%d/%m/%y %H:%M:%S')

            data = {
                "records": [
                    {
                        "fields": {
                            # "region": region,
                            "suburb": suburb,
                            "subject": subject,
                            "type": type,
                            # "description": description,
                            "incidentid": incidentid,
                            "lattitude": lattitude,
                            "longitude": longitude,
                            "lastupdate": lastupdate
                        }
                    }
                ]
            }

            r = requests.post(endpoint, json=data, headers=headers)

            print(r.json())