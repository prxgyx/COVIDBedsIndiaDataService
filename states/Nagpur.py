import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
from states.State import State
import json


class Nagpur(State):
    def __init__(self, test_prefix=None):
        self.state_name = "Nagpur"
        super().__init__()
        self.source_url = "http://nsscdcl.org/covidbeds/AvailableHospitalsNew.jsp"
        self.main_sheet_name = "Nagpur"
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
        self.icu_beds_column = "TOTAL_ICU_BEDS_OCCUPIED"
        self.vent_beds_column = "TOTAL_VENTILATOR_BEDS_OCCUPIED"

    def get_data_from_source(self):
        s_no = 0
        response = requests.get(self.source_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        output_json = []
        rows = soup.find('table', class_='table table-bordered table-striped').find(
            'tbody').find_all('tr')

        for tr in rows:
            hospital = tr.find('b').text
            phone_number = tr.find('a').text
            hospita_address = tr.find_all('td')[1].text.replace(hospital, "").replace(phone_number,
                                                                                      "").strip()

            innerURL = 'http://nsscdcl.org/covidbeds/GetBedsData.jsp?hname=' + hospital.replace(" ",
                                                                                                "%20")
            innerresponse = requests.get(innerURL)
            innersoup = BeautifulSoup(innerresponse.text, 'html.parser')

            hospital_data = {}
            if bool(json.loads(str(innersoup))):
                hospital_data = json.loads(str(innersoup))[0]

            hosp = {
                "SNO": s_no + 1,
                "HOSPITAL_NAME": str(hospital or ""),
                "ADDRESS": str(hospita_address or "Nagpur"),
                "HOSPITAL_NUMBER": str(phone_number or ""),
                "TOTAL_OXYGEN_BEDS_AVAILABLE": str(hospital_data.get('a_O2') or "0"),
                "TOTAL_OXYGEN_BEDS_OCCUPIED": str(hospital_data.get('o_O2') or "0"),
                "TOTAL_WITHOUT_OXYGEN_BEDS_AVAILABLE": str(hospital_data.get('a_nonO2') or "0"),
                "TOTAL_WITHOUT_OXYGEN_BEDS_OCCUPIED": str(hospital_data.get('o_nonO2') or "0"),
                "TOTAL_ICU_BEDS_AVAILABLE": str(hospital_data.get('a_ICU') or "0"),
                "TOTAL_ICU_BEDS_OCCUPIED": str(hospital_data.get('o_ICU') or "0"),
                "TOTAL_VENTILATOR_BEDS_AVAILABLE": str(hospital_data.get('a_ventilator') or "0"),
                "TOTAL_VENTILATOR_BEDS_OCCUPIED": str(hospital_data.get('o_ventilator') or "0")
            }
            s_no = s_no + 1
            output_json.append(hosp)

        return pd.DataFrame(output_json)
