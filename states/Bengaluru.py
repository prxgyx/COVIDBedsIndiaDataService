import time
import requests
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.webdriver.common.by import By
import pandas as pd
import logging
import json
from states.State import State
import re


regex = '^[0-9]+$'



class Bengaluru(State):

    def __init__(self, test_prefix=None):
        self.state_name = "Bengaluru"
        super().__init__()
        self.source_url = "https://apps.bbmpgov.in/Covid19/en/bedstatus.php"
        self.main_sheet_name = "Bengaluru"
        if test_prefix:
            self.main_sheet_name = test_prefix + self.main_sheet_name
        self.unique_columns = ["HOSPITAL_NAME"]
        self.old_info_columns = ["LOCATION"]
        self.sheet_url = self.stein_url + "/" + self.main_sheet_name
        # Fetching it here because need number of records in the Class
        # need number of records because bulk delete API throws error entity too large
        logging.info("Fetching data from Google Sheets")
        self.sheet_response = requests.get(self.sheet_url).json()
        self.number_of_records = len(self.sheet_response)
        logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))
        self.icu_beds_column = "ALLOCATED_ICU"
        self.vent_beds_column = "ALLOCATED_ICU_WITH_VENTILATOR"

    def get_data_from_source(self):
        fireFoxOptions = webdriver.FirefoxOptions()
        fireFoxOptions.set_headless()
        browser = webdriver.Firefox(firefox_options=fireFoxOptions)

        browser.get(self.source_url)
        WebDriverWait(browser, 20).until(EC.frame_to_be_available_and_switch_to_it((By.TAG_NAME,"iframe")))
        time.sleep(10)

        element=browser.find_elements_by_css_selector('div[class="tableExContainer"]')[-1]

        final_results, hosp_encountered = [], []

         
        for j in range(10):
            table_body = element.find_elements_by_css_selector('div[class="bodyCells"] > div > div > div > div')

            check_forward=False
            final_indexes = []
            counter, prev_counter, indexes =0, -1, []

            for x in table_body:
                if counter-1!=prev_counter:
                    if len(indexes)>0:
                        final_indexes.append(indexes)
                        indexes = []
                if not re.search(regex, x.text):
                    prev_counter = counter
                    indexes.append(counter)

                counter+=1
            for index_list in final_indexes:
                hosp_count = len(index_list)


                for idx in index_list:
                    hosp_info = [table_body[idx].text]
                    for k in range(1, 17):
                        value = table_body[(hosp_count*k)+idx].text
                        if re.search(regex, value):
                            hosp_info.append(value)
                    if len(hosp_info)==17:
                        if hosp_info[0] not in hosp_encountered:
                            hosp_encountered.append(hosp_info[0])
                            final_results.append(hosp_info)



            for table_obj in table_body:
                if len(final_results) > 20*(j+1):
                    if table_obj.text == final_results[20*(j+1)][0]:
                        break
                else:
                    if table_obj.text== final_results[-1][0]:
                        break
                

            browser.execute_script("arguments[0].scrollIntoView(true);", table_obj)
            time.sleep(2)
            
        return pd.DataFrame(final_results, columns=["HOSPITAL_NAME", "ALLOCATED_GENERAL","ALLOCATED_HDU", 
            "ALLOCATED_ICU", "ALLOCATED_ICU_WITH_VENTILATOR", "ADMITTED_GENERAL", "ADMITTED_HDU", "ADMITTED_ICU",
            "ADMITTED_ICU_WITH_VENTILATOR", "BLOCKED_GENERAL", "BLOCKED_HDU","BLOCKED_ICU", "BLOCKED_ICU_WITH_VENTILATOR",
            "NET_AVAILABLE_GENERAL","NET_AVAILABLE_HDU", "NET_AVAILABLE_ICU", "NET_AVAILABLE_ICU_WITH_VENTILATOR"]).drop_duplicates()