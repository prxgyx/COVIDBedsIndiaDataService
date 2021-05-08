import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
from states.State import State


class Surat(State):
    def __init__(self, test_prefix=None):
        super().__init__()
        self.stein_url = "https://stein.hamaar.cloud/v1/storages/6094e879423e213eb82fd384"
        self.source_url = "http://office.suratsmartcity.com/SuratCOVID19/Home/COVID19BedAvailabilityDetails"
        self.state_name = "Surat"
        self.main_sheet_name = "Surat"
        if test_prefix:
            self.main_sheet_name = test_prefix + self.main_sheet_name
        self.old_info_columns = ["LOCATION"]
        self.sheet_url = self.stein_url + "/" + self.main_sheet_name
        # Fetching it here because need number of records in the Class
        # need number of records because bulk delete API throws error entity too large
        logging.info("Fetching data from Google Sheets")
        self.sheet_response = requests.get(self.sheet_url).json()
        self.number_of_records = len(self.sheet_response)
        logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))
        self.icu_beds_column = "TOTAL_OXYGEN_BEDS_AVAILABLE"
        self.vent_beds_column = "TOTAL_VENTILATOR_BEDS_AVAILABLE"

    def get_data_from_source(self):
        s_no = 0
        response = requests.get(self.source_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        output_json = []
        rows = soup.find_all('div', attrs={'class': 'card custom-card'})

        for tr in rows:
            beds_list = tr.find_all('ul', class_='list-unstyled list-customised clearfix')[0].find_all('li')

            hospital_name = tr.find('a', class_='hospital-info').text.replace(" Contact", "")
            hospital_address = tr.find('a', href=True)['href'].split('\',\'')[1].strip("\'")
            hospital_number = tr.find('a', href=True)['href'].split('\',\'')[2].strip("\');")
            hospital_type = tr.find('span', class_='badge badge-success').text
            last_updated = tr.find('span', class_='badge badge-lastupdated').text.replace("Last Updated- ", "")
            total_beds = tr.find('span', class_='count-text').text.replace("Total Beds -", "")
            total_beds_available = tr.find('span', class_='count-text pr-2').text.replace("Total Vacant -", "")
            total_ward_beds_available = beds_list[0].find('div', class_='count-text').text
            total_oxygen_beds_available = beds_list[1].find('div', class_='count-text').text
            total_bipap_beds_available = beds_list[2].find('div', class_='count-text').text
            total_ventilator_beds_available = beds_list[3].find('div', class_='count-text').text

            hosp = {
                "SNO": s_no + 1,
                "HOSPITAL_NAME": str(hospital_name or ""),
                "HOSPITAL_ADDRESS": str(hospital_address or ""),
                "DISTRICT": "",
                "HOSPITAL_NUMBER": str(hospital_number or ""),
                "HOSPITAL_TYPE": str(hospital_type or ""),
                "LAST_UPDATED": str(last_updated or ""),
                "TOTAL_BEDS": str(total_beds or ""),
                "TOTAL_BEDS_AVAILABLE": str(total_beds_available or ""),
                "TOTAL_WARD_BEDS_AVAILABLE": str(total_ward_beds_available or ""),
                "TOTAL_OXYGEN_BEDS_AVAILABLE": str(total_oxygen_beds_available or ""),
                "TOTAL_BIPAP_BEDS_AVAILABLE": str(total_bipap_beds_available or ""),
                "TOTAL_VENTILATOR_BEDS_AVAILABLE": str(total_ventilator_beds_available or "")
            }
            s_no = s_no + 1
            output_json.append(hosp)

        return pd.DataFrame(output_json)
