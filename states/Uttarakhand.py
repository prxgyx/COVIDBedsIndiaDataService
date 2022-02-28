import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
from states.State import State


class Uttarakhand(State):
    def __init__(self, test_prefix=None):
        self.state_name = "Uttarakhand"
        super().__init__()
        self.source_url = "https://covid19.uk.gov.in/bedssummary.aspx"
        self.main_sheet_name = "Uttarakhand"
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

    def get_data_from_source(self):
        s_no = 0
        response = requests.get(self.source_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        output_json = []

        for tr in soup.find_all('tr')[2:]:
            tds = tr.find_all('td')
            hosp = {
                "SNO": s_no + 1,
                "DISTRICT": tds[0].find_all('span')[0].text,
                "HOSPITAL_NAME": tds[1].find_all('span')[0].text,
                "TYPE": tds[1].find_all('span')[1].text,
                "NODAL_OFFICER_NAME": tds[2].find_all('span')[0].text,
                "NODAL_OFFICER_NUMBER": tds[2].find_all('span')[1].text,
                "BEDS_WITHOUT_OXYGEN_AVAILABLE": tds[3].find_all('span')[0].text,
                "BEDS_WITHOUT_OXYGEN_TOTAL": tds[3].find_all('span')[1].text,
                "BEDS_WITH_OXYGEN_AVAILABLE": tds[4].find_all('span')[0].text,
                "BEDS_WITH_OXYGEN_TOTAL": tds[4].find_all('span')[1].text,
                "ICU_BEDS_AVAILABLE": tds[5].find_all('span')[0].text,
                "ICU_BEDS_TOTAL": tds[5].find_all('span')[0].text,
                "LAST_UPDATED": tds[6].find_all('span')[0].text
            }
            s_no = s_no + 1
            output_json.append(hosp)

        return pd.DataFrame(output_json)

    def tag_critical_care(self, merged_loc_df):
        logging.info("Tagged critical care")
        merged_loc_df["HAS_ICU_BEDS"] = merged_loc_df.apply(
            lambda row: int(row["ICU_BEDS_TOTAL"]) > 0, axis=1)
        return merged_loc_df
