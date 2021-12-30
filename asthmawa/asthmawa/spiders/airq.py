import scrapy
import requests


class AirqSpider(scrapy.Spider):
    name = 'airq'
    allowed_domains = ['der.wa.gov.au']
    start_urls = ['https://www.der.wa.gov.au/your-environment/air/air-quality-index']

    def parse(self, response):
        airtable_base_id = 'appuetjbGSC7dJNHl'
        airtable_table_name = 'Air Quality Index'
        airtable_api_key = 'keyaeXOkK8tB7sjZ1'
        endpoint = f'https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_name}'
        headers = {
            "Authorization": f"Bearer {airtable_api_key}",
            "Content-Type": "application/json"
        }

        tables = response.css('td.tr')

        for table in tables:
            locationraw = table.css('b:nth-child(1)::text').get()
            status = str(table.css('b:nth-child(2)::text').get()).title()

            if locationraw is not None:

                if "Particles" in locationraw:
                    location = table.css('b:nth-child(1)::text').get()[:-19]
                else:
                    location = table.css('b:nth-child(1)::text').get()[:-9]

                # print(location, status)

                data = {
                    "records": [
                        {
                            "fields": {
                                "location": location,
                                "status": status
                            }
                        }
                    ]
                }

                r = requests.post(endpoint, json=data, headers=headers)

                print(r.json())