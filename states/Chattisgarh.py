import json
import requests
from selenium import webdriver
import time
import pandas as pd
from states.State import State


class Chattisgarh(State):

	def __init__(self):
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/6089833203eef38338d05a73"
		self.source_url = "https://cg.nic.in/health/covid19/RTPBedAvailable.aspx"
		self.custom_sheet_name = "Sheet10"
		self.main_sheet_name = "Chattisgarh"

	def get_dummy_data(self):
		dummy_data = [
			{
	            "SNO": "1",
	            "DISTRICT": "BALOD",
	            "HOSPITAL_NAME": "Livelyhood College Balod",
	            "CONTACT": "Dr. Indra Kumar Chandrakar 9826105264",
	            "CATEGORY": "Covid Care Center",
	            "BEDS_TOTAL": "274",
	            "OXYGEN_BEDS_TOTAL": "10",
	            "OXYGEN_BEDS_VACANT": "0",
	            "NONOXYGEN_BEDS_TOTAL": "264",
	            "NONOXYGEN_BEDS_VACANT": "65",
	            "ISOLATION_BEDS_OUTSIDE_HOSPITAL_TOTAL": "0",
	            "ISOLATION_BEDS_OUTSIDE_HOSPITAL_VACANT": "0",
	            "HDU_BEDS_TOTAL": "0",
	            "HDU_BEDS_VACANT": "0",
	            "ICU_BEDS_TOTAL": "0",
	            "ICU_BEDS_VACANT": "0",
	            "VENTILATORS_TOTAL": "0",
	            "VENTILATORS_VACANT": "0",
	            "BEDS_VACANT": "65",
	            "EMPANELLED_IN_AYUSHMAN": "Yes",
	            "LAST_UPDATED_DATE": "28/04/2021",
	            "LAST_UPDATED_TIME": "10:37"
        	}
		]
		return dummy_data

	def get_data_from_source(self):
		output_json =[]

		fireFoxOptions = webdriver.FirefoxOptions()
		fireFoxOptions.set_headless()
		browser = webdriver.Firefox(firefox_options=fireFoxOptions)
		page_retrieved, retries = False, 0

		# in case of heavy traffic the page fails to load so retrying till it loads
		while not page_retrieved and retries < 5:
			try:
				browser.get(self.source_url)
				page_retrieved = True
			except:
				print("Page failed to load. Retrying")
				retries +=1
				time.sleep(10)

		if retries >=5:
			return [] 

		# Clicking the Show data button
		browser.find_element_by_css_selector("input[value='Show']").click()

		# extracting all table rows
		elements = browser.find_elements_by_css_selector("table.table > tbody > tr")

		# removing the header and last total entry from the iteration
		for element in elements[1:-1]:
			tds = element.find_elements_by_css_selector('td')

			json_obj = {
			"SNO": tds[0].text,
			"DISTRICT": tds[1].text,
			"HOSPITAL_NAME": tds[2].text,
			"CONTACT": tds[3].text,
			"CATEGORY": tds[4].text,
			"BEDS_TOTAL": tds[5].text,
			"OXYGEN_BEDS_TOTAL": tds[6].text,
			"OXYGEN_BEDS_VACANT": tds[7].text,
			"NONOXYGEN_BEDS_TOTAL": tds[8].text,
			"NONOXYGEN_BEDS_VACANT": tds[9].text,
			"ISOLATION_BEDS_OUTSIDE_HOSPITAL_TOTAL": tds[10].text,
			"ISOLATION_BEDS_OUTSIDE_HOSPITAL_VACANT": tds[11].text,
			"HDU_BEDS_TOTAL": tds[12].text,
			"HDU_BEDS_VACANT": tds[13].text,
			"ICU_BEDS_TOTAL": tds[14].text,
			"ICU_BEDS_VACANT": tds[15].text,
			"VENTILATORS_TOTAL": tds[16].text,
			"VENTILATORS_VACANT": tds[17].text,
			"BEDS_VACANT": tds[18].text,
			"EMPANELLED_IN_AYUSHMAN": tds[19].text,
			"LAST_UPDATED_DATE": tds[20].text,
			"LAST_UPDATED_TIME": tds[21].text,
			}
			output_json.append(json_obj)
		return output_json




	def tag_critical_care(self, merged_loc_df):
		merged_loc_df["HAS_ICU_BEDS"] = merged_loc_df.apply(lambda row: True 
												if int(row["ICU_BEDS_TOTAL"]) > 0 else False, axis=1)

		merged_loc_df["HAS_VENTILATORS"] = merged_loc_df.apply(lambda row: True 
												if int(row["VENTILATORS_TOTAL"]) > 0 else False, axis=1)
		
		return merged_loc_df

