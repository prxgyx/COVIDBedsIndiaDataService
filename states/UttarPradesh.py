import urllib3
import requests
from states.State import State
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.webdriver.common.by import By
import numpy as np
from states.notification.TelegramBot import TelegramBot


covidbedsbot = TelegramBot()



class UttarPradesh(State):

	def __init__(self, test_prefix=None):
		super().__init__()
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/609fc7dde75f9ccdd696eb35"
		self.source_url = "https://beds.dgmhup-covid19.in/EN/covid19bedtrack"
		self.main_sheet_name = "Uttar Pradesh"
		if test_prefix:
			self.main_sheet_name = test_prefix + self.main_sheet_name
		self.state_name = "Uttar Pradesh"
		self.sheet_url = self.stein_url + "/" + self.main_sheet_name
		# Fetching it here because need number of records in the Class
		# need number of records because bulk delete API throws error entity too large
		logging.info("Fetching data from Google Sheets")
		self.sheet_response = requests.get(self.sheet_url).json()
		self.number_of_records = len(self.sheet_response)
		logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))


	def get_data_from_source(self):
		fireFoxOptions = webdriver.FirefoxOptions()
		fireFoxOptions.set_headless()
		browser = webdriver.Firefox(firefox_options=fireFoxOptions)

		browser.get(self.source_url)

		all_options = browser.find_elements_by_css_selector('select[id="MainContent_EN_ddDistrict"] > option')
		all_districts_list = list(range(1, len(all_options)+1))

		output_json = []
		retry_counter = {}

		while len(all_districts_list)>0:
			try:
				i = all_districts_list.pop(0)
				if i in retry_counter:
					retry_counter[i]+=1
				else:
					retry_counter[i] = 0
				option_selector = browser.find_element_by_css_selector('select[id="MainContent_EN_ddDistrict"]')
				option_selector.click()
				district = option_selector.find_elements_by_css_selector('option')[i].text
				option_selector.find_elements_by_css_selector('option')[i].click()
				browser.find_element_by_css_selector('input[value="Submit"]').click()
				time.sleep(4)
				table_obj = browser.find_elements_by_css_selector('table[class="style92"] > tbody')[1]
				all_trs = table_obj.find_elements_by_css_selector('tr')[1:]
				n=3
				all_tr_batches = [all_trs[i * n:(i + 1) * n] for i in range((len(all_trs) + n - 1) // n )] 

				
				for tr_batch in all_tr_batches:
					json_hosp = {}

					tds = tr_batch[0].find_elements_by_css_selector('td')[1:]
					span_elements = tds[0].find_elements_by_css_selector('span')
					json_hosp["HOSPITAL_NAME"] = span_elements[0].text
					json_hosp["DISTRICT"] = district
					json_hosp["TYPE"] = span_elements[1].text
					if len(span_elements) > 2:
						json_hosp["ADDRESS"] = span_elements[2].text


					json_hosp["TOTAL"] = tds[1].text
					json_hosp["VACANT"] = tds[3].text
					json_hosp["LAST_UPDATED"] = tds[4].text
					span_elements = tr_batch[1].find_elements_by_css_selector('td')[1].find_elements_by_css_selector('span')
					if len(span_elements)>1:
						json_hosp["ADDRESS"] = span_elements[0].text
						json_hosp["CONTACT"] = span_elements[1].text
					else:
						json_hosp["CONTACT"]= span_elements[0].text
					output_json.append(json_hosp)
			except Exception as e:
				print(e)
				if retry_counter[i]<3:
					all_districts_list.append(i)

				time.sleep(10)

		return pd.DataFrame(output_json)


	def get_location_from_master(self, govt_data_df, sheet_data_df):
		unique_columns_lower = []
		for unique_column in self.unique_columns:
			sheet_data_df[unique_column] = sheet_data_df[unique_column].str.strip()
			govt_data_df[unique_column] = govt_data_df[unique_column].str.strip()
			sheet_data_df[unique_column+"_lower"] = sheet_data_df[unique_column].str.lower()
			govt_data_df[unique_column+"_lower"] = govt_data_df[unique_column].str.lower()
			unique_columns_lower.append(unique_column+"_lower")


		sheet_data_df_subset = sheet_data_df[unique_columns_lower+ self.old_info_columns + ["UID", "IS_NEW_HOSPITAL"]]

		bool_values = {"TRUE":True, "FALSE": False}
		sheet_data_df_subset["IS_NEW_HOSPITAL"] = sheet_data_df_subset["IS_NEW_HOSPITAL"].map(bool_values)

		merged_loc_df = pd.merge(govt_data_df, sheet_data_df_subset, 
									on=unique_columns_lower, how="left")
		merged_loc_df["IS_NEW_HOSPITAL"] = merged_loc_df["IS_NEW_HOSPITAL"].fillna(value=True)

		merged_loc_df = merged_loc_df.fillna('')
		merged_loc_df = merged_loc_df.drop(unique_columns_lower, axis=1)
		merged_loc_df["STEIN_ID"] = self.state_name
		merged_loc_df = merged_loc_df.drop_duplicates(self.unique_columns)

		merged_loc_df = self.get_uid_lastsynced(merged_loc_df)	
		return merged_loc_df.to_dict('records')


	def push_data(self):

		logging.info("Pushing for state - "+ self.state_name)

		logging.info("Fetching data from source")
		govt_data_df = self.get_data_from_source()
		sheet_data_df = pd.DataFrame(self.sheet_response)

		if len(govt_data_df) > 0:
			self.get_error_message(self.sheet_response)

			logging.info("Fetching location from master sheet")
			location_tagged_data = self.get_location_from_master(govt_data_df, sheet_data_df)

			if len(sheet_data_df)*.9 > len(location_tagged_data):
				failure_reason = "Row count with the scraped data is low, can cause data loss, Omitting writing to main file"
				logging.info(failure_reason)
				covidbedsbot.send_message(self.error_msg_info(failure_reason, sheet_data_df))
			else:
				self.write_temp_file(sheet_data_df)

				delete_data_response = self.delete_data_from_sheets()

				# Successful delete
				if "clearedRowsCount" in delete_data_response:
					self.push_data_to_sheets(location_tagged_data, 50)
					covidbedsbot.send_message(self.success_msg_info(sheet_data_df, location_tagged_data))
					covidbedsbot.send_local_file("tmp_{}".format(self.state_name))
				else:
					failure_reason = "Error deleting data from sheets - {}".format(delete_data_response)
					covidbedsbot.send_message(self.error_msg_info(failure_reason, sheet_data_df))

		else:
			failure_reason = "No data retrieved from url"
			logging.info(failure_reason)
			covidbedsbot.send_message(self.error_msg_info(failure_reason, sheet_data_df))
