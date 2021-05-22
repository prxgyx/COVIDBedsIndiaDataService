import logging

import pandas as pd
import requests
import time
from selenium import webdriver

from states.State import State


class Ahmedabad(State):
    def __init__(self, test_prefix=None):
        super().__init__()
        self.stein_url = ""
        self.source_url = "https://ahmedabadcity.gov.in/portal/web?requestType=ApplicationRH&actionVal=loadCoronaRelatedDtls&queryType=Select&screenId=114"
        self.state_name = "Ahmedabad"
        self.main_sheet_name = "Ahmedabad"
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
        self.icu_beds_column = "ICU_BEDS_WITHOUT_VENTILATOR"
        self.vent_beds_column = "ICU_BEDS_WITH_VENTILATOR"

    def get_data_from_source(self):
        output_json = []
        fireFoxOptions = webdriver.FirefoxOptions()
        browser = webdriver.Firefox(firefox_options=fireFoxOptions)
        browser.get(self.source_url)
        browser.get(self.source_url)

        time.sleep(20)
        html_str = str(browser.find_element_by_css_selector("body").get_attribute('innerHTML'))
        odd_elements = browser.find_elements_by_css_selector('tr[class="odd"]')
        even_elements = browser.find_elements_by_css_selector('tr[class="even"]')

        hosp_list = even_elements + odd_elements
        s_no = 0

        for element in hosp_list:
            hospital_data = element.find_elements_by_css_selector('td')
            hospital_name = hospital_data[1].text
            hospital_zone = hospital_data[2].text
            total_occupied_beds = hospital_data[3].text
            isolation_beds = hospital_data[4].text
            total_beds_with_oxygen = hospital_data[5].text
            icu_beds_without_ventilator = hospital_data[6].text
            icu_with_ventilator = hospital_data[7].text
            nodal_officer_name = hospital_data[8].text
            nodal_officer_number = hospital_data[9].text
            last_updated = hospital_data[10].text

            hosp = {
                "SNO": s_no + 1,
                "HOSPITAL_NAME": str(hospital_name or ""),
                "HOSPITAL_ZONE": str(hospital_zone or ""),
                "TOTAL_OCCUPIED_BEDS": str(total_occupied_beds or ""),
                "ISOLATION_BEDS": str(isolation_beds or "0"),
                "TOTAL_BEDS_WITH_OXYGEN": str(total_beds_with_oxygen or "0"),
                "ICU_BEDS_WITHOUT_VENTILATOR": str(icu_beds_without_ventilator or "0"),
                "ICU_BEDS_WITH_VENTILATOR": str(icu_with_ventilator or "0"),
                "NODAL_OFFICER_NAME": str(nodal_officer_name or "0"),
                "NODAL_OFFICER_NUMBER": str(nodal_officer_number or "0"),
                "LAST_UPDATED": str(last_updated or "0"),
            }
            s_no = s_no + 1
            output_json.append(hosp)

        browser.close()
        browser.quit()
        return pd.DataFrame(output_json)
