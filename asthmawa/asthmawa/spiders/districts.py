import scrapy
import json
import requests


class DistrictsSpider(scrapy.Spider):
    name = 'districts'
    allowed_domains = ['emergency.wa.gov.au']
    start_urls = ['https://www.emergency.wa.gov.au/data/BOM_Fire_Weather_Subdistricts.json']

    def parse(self, response):
        airtable_base_id = 'appuetjbGSC7dJNHl'
        airtable_table_name = 'Emergency WA - Subdistricts'
        airtable_api_key = 'keyaeXOkK8tB7sjZ1'
        endpoint = f'https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_name}'
        headers = {
            "Authorization": f"Bearer {airtable_api_key}",
            "Content-Type": "application/json"
        }

        incidents = json.loads(response.body)['features']

        for incident in incidents:
            district = incident.get('properties').get('DIST_NAME').title()
            subdistrict = incident.get('properties').get('SDIST_NAME').title()
            id = int(incident.get('properties').get('OBJECTID'))

            data = {
                "records": [
                    {
                        "fields": {
                            "id": id,
                            "district": district,
                            "subdistrict": subdistrict
                        }
                    }
                ]
            }

            r = requests.post(endpoint, json=data, headers=headers)

            print(r.json())