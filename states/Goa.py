# -*- coding: utf-8 -*-
import logging

import requests
import pandas as pd

import urllib3
from bs4 import BeautifulSoup

from states.State import State


class Goa(State):
    def __init__(self, test_prefix=None):
        self.state_name = "Goa"
        super().__init__()
        self.source_url = "https://goaonline.gov.in/beds"
        self.main_sheet_name = "Goa"
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
        self.icu_beds_column = "TOTAL_ICU_BEDS"
        self.vent_beds_column = "TOTAL_ICU_BEDS"

    def get_data_from_source(self):
        http = urllib3.PoolManager()
        response = requests.get(
            self.source_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        output_json = []
        rows = soup.find('table', class_='table table-bordered table-striped app-tables').find_all(
            'tr')

        for tr in rows[1:-2]:
            hospital_data = tr.find_all('td')

            s_no = hospital_data[0].text
            hospital_name = hospital_data[1].text
            total_beds = hospital_data[2].text
            total_beds_available = hospital_data[3].text
            total_icu_beds = hospital_data[4].text
            total_icu_beds_available = hospital_data[5].text
            last_updated = hospital_data[6].text

            hosp = {
                "SNO": str(s_no or ""),
                "HOSPITAL_NAME": str(hospital_name or ""),
                "TOTAL_BEDS": str(total_beds or ""),
                "TOTAL_BEDS_AVAILABLE": str(total_beds_available or "0"),
                "TOTAL_ICU_BEDS": str(total_icu_beds or "0"),
                "TOTAL_ICU_BEDS_AVAILABLE": str(total_icu_beds_available or "0"),
                "LAST_UPDATED": str(last_updated or "0"),
            }

            output_json.append(hosp)

        return pd.DataFrame(output_json)
