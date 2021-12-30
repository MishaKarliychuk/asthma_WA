import scrapy
import json
import requests


class LocalgovsSpider(scrapy.Spider):
    name = 'localgovs'
    allowed_domains = ['emergency.wa.gov.au']
    start_urls = ['https://www.emergency.wa.gov.au/data/TFB_Local_Governments.json']



    def parse(self, response):
        airtable_base_id = 'appuetjbGSC7dJNHl'
        airtable_table_name = 'Emergency WA - Local Government'
        airtable_api_key = 'keyaeXOkK8tB7sjZ1'
        endpoint = f'https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_name}'
        headers = {
            "Authorization": f"Bearer {airtable_api_key}",
            "Content-Type": "application/json"
        }

        govs = json.loads(response.body)['features']

        for gov in govs:
            geometry = gov.get('geometry').get('coordinates')
            for coords in geometry:
                for item in coords:
                    lattitude = str(item[1])
                    longitude = str(item[0])

            longname = gov.get('properties').get('LONG_NAME').title()
            name = gov.get('properties').get('NAME').title()
            id = int(gov.get('properties').get('OBJECTID'))

            print(id, longname, name, lattitude, longitude)

            # data = {
            #     "records": [
            #         {
            #             "fields": {
            #                 "id": id,
            #                 "longname": longname,
            #                 "name": name
            #             }
            #         }
            #     ]
            # }
            #
            # r = requests.post(endpoint, json=data, headers=headers)
            #
            # print(r.json())