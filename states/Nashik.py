import logging

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup

from states.State import State


class Nashik(State):

    def __init__(self, test_prefix=None):
        super().__init__()
        self.state_name = "Nashik"
        self.stein_url = "https://stein.hamaar.cloud/v1/storages/608efad8423e2153ac2fd383"
        self.source_url = "http://covidcbrs.nmc.gov.in/home/searchHosptial"
        self.main_sheet_name = "Nashik"
        if test_prefix:
            self.main_sheet_name = test_prefix + self.main_sheet_name
        self.unique_columns = ["HOSPITAL_NAME", "DIVISION"]
        self.sheet_url = self.stein_url + "/" + self.main_sheet_name
        # Fetching it here because need number of records in the Class
        # need number of records because bulk delete API throws error entity too large
        logging.info("Fetching data from Google Sheets")
        self.sheet_response = requests.get(self.sheet_url).json()
        self.number_of_records = len(self.sheet_response)
        logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))
        self.icu_beds_column = "ICU_BEDS_TOTAL"
        self.vent_beds_column = "VENTILATOR_TOTAL"

    def get_data_from_source(self):
        s_no = 0
        response = requests.get(self.source_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        output_json = []
        rows = soup.find('table',
                         attrs={'class': 'table table-striped table-bordered table-hover dataTable no-footer'}).find(
            'tbody').find_all('tr')

        for tr in rows:
            tds = tr.find_all('td')
            datatarget_model = tds[1].find_all('a')[0]['data-target']
            elements = soup.find_all('div', attrs={'id': datatarget_model.strip("'").strip("#")})
            element_with_list = elements[0].find('div', class_='modal-body')
            list_elements = element_with_list.find_all('li')

            hosp = {
                "SNO": s_no+1,
                "HOSPITAL_NAME": list_elements[0].find_all('label')[1].text,
                "DIVISION": list_elements[1].find_all('label')[1].text,
                "INFO": list_elements[2].find_all('label')[1].text,
                "HOSPITAL_ADDRESS": list_elements[3].find_all('label')[1].text,
                "DOCTOR_NAME": list_elements[4].find_all('label')[1].text,
                "HOSPITAL_NUMBER": list_elements[5].find_all('label')[1].text,
                "HOSPITAL_LANDLINE_NUMBER": list_elements[6].find_all('label')[1].text,
                "HOSPITAL_COORDINATORS": list_elements[7].find_all('label')[1].text,
                "HOSPITAL_COORDINATORS_NUMBER": list_elements[8].find_all('label')[1].text,
                "AUDITOR_NAME": list_elements[9].find_all('label')[1].text,
                "AUDITOR_NUMBER": list_elements[10].find_all('label')[1].text,
                "NMC_RESERVED_BED_TOTAL": list_elements[11].find_all('label')[1].text,
                "NMC_RESERVED_BED_AVAILABLE": list_elements[12].find_all('label')[1].text,
                "NMC_RESERVED_BED_VACANT": list_elements[13].find_all('label')[1].text,
                "TYPE": tds[2].text,
                "BEDS_WITHOUT_OXYGEN_TOTAL": tds[3].text,
                "BEDS_WITHOUT_OXYGEN_AVAILABLE": tds[4].text.strip(),
                "BEDS_WITH_OXYGEN_TOTAL": tds[5].text,
                "BEDS_WITH_OXYGEN_AVAILABLE": tds[6].text.strip(),
                "ICU_BEDS_TOTAL": tds[7].text,
                "ICU_BEDS_AVAILABLE": tds[8].text.strip(),
                "VENTILATOR_TOTAL": tds[9].text,
                "VENTILATOR_AVAILABLE": tds[10].text.strip()
            }
            s_no = s_no + 1

            output_json.append(hosp)

        return pd.DataFrame(output_json)
