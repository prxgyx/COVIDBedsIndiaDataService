import logging
import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from states.State import State
import json


class Thane(State):
    def __init__(self, test_prefix=None):
        super().__init__()
        self.stein_url = "https://stein.hamaar.cloud/v1/storages/609fc78ae75f9c111f96eb34"
        self.source_url = "https://covidbedthane.in/HospitalInfo/showindex"
        self.state_name = "Thane"
        self.main_sheet_name = "Thane"
        if test_prefix:
            self.main_sheet_name = test_prefix + self.main_sheet_name
        self.unique_columns = ["HOSPITAL_NAME"]
        self.old_info_columns = ["LOCATION", "LAT", "LONG"]
        self.sheet_url = self.stein_url + "/" + self.main_sheet_name
        # Fetching it here because need number of records in the Class
        # need number of records because bulk delete API throws error entity too large
        logging.info("Fetching data from Google Sheets")
        self.sheet_response = requests.get(self.sheet_url).json()
        self.number_of_records = len(self.sheet_response)
        logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))
        self.icu_beds_column = "TOTAL_ICU_BEDS_AVAILABLE"
        self.vent_beds_column = "TOTAL_ICU_BEDS_AVAILABLE"

    def get_data_from_source(self):
        http = urllib3.PoolManager()
        s_no = 0
        response = requests.get(self.source_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        output_json = []
        rows = soup.find_all('div', class_='col-xl-6 col-md-12 col-lg-12 col-sm-12 m-b-6')

        for tr in rows:
            hospital_name = tr.find('div', class_='text-center text-white social mt-3').find('h4').text
            if tr.find('div', class_='text-center text-white social mt-3').find('span') is not None:
                hospital_number = str(tr.find('div', class_='text-center text-white social mt-3').find('span').text.replace("For More Details :- ", "") or "")
            else:
                hospital_number = ""

            total_beds = "0" if "error" in tr.find_all('strong')[0].find('b').text else tr.find_all('strong')[0].find('b').text
            total_beds_occupied = "0" if "error" in tr.find_all('strong')[1].find('b').text else tr.find_all('strong')[1].find('b').text
            total_beds_available = "0" if "error" in tr.find_all('strong')[2].find('b').text else tr.find_all('strong')[2].find('b').text
            total_icu_beds_available = "0" if "error" in tr.find_all('strong')[3].find('b').text else tr.find_all('strong')[3].find('b').text
            total_non_icu_beds_available = "0" if "error" in tr.find_all('strong')[4].find('b').text else tr.find_all('strong')[4].find('b').text

            hosp = {
                "SNO": s_no + 1,
                "HOSPITAL_NAME": str(hospital_name or ""),
                "HOSPITAL_NUMBER": str(hospital_number or ""),
                "TOTAL_BEDS": str(total_beds or ""),
                "TOTAL_BEDS_OCCUPIED": str(total_beds_occupied or ""),
                "TOTAL_BEDS_AVAILABLE": str(total_beds_available or "0"),
                "TOTAL_ICU_BEDS_AVAILABLE": str(total_icu_beds_available or "0"),
                "TOTAL_NON_ICU_BEDS_AVAILABLE": str(total_non_icu_beds_available or "0"),
            }
            s_no = s_no + 1
            output_json.append(hosp)

        return pd.DataFrame(output_json)
