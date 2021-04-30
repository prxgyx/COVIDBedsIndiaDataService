import json
import requests
from selenium import webdriver
import time
import pandas as pd
from states.State import State


class Telangana(State):

	def __init__(self):
		self.state_name = "Telangana"
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/6089829403eef36d93d05a6f"
		self.source_url = "http://164.100.112.24/SpringMVC/Hospital_Beds_Statistic_Bulletin_citizen.htm"
		self.custom_sheet_name = "Sheet16"
		self.main_sheet_name = "Telangana"
		super().__init__()


	def get_dummy_data(self):
		dummy_data = [
			{
	            "SNO": "1",
	            "DISTRICT": "Adilabad",
	            "HOSPITAL_NAME": "A.D.B. HOSPITALS",
	            "CONTACT": "8555098068",
	            "REGULAR_BEDS_TOTAL": "11",
	            "REGULAR_BEDS_OCCUPIED": "3",
	            "REGULAR_BEDS_VACANT": "8",
	            "OXYGEN_BEDS_TOTAL": "5",
	            "OXYGEN_BEDS_OCCUPIED": "5",
	            "OXYGEN_BEDS_VACANT": "0",
	            "ICU_BEDS_TOTAL": "3",
	            "ICU_BEDS_OCCUPIED": "3",
	            "ICU_BEDS_VACANT": "0",
	            "TOTAL": "19",
	            "OCCUPIED": "11",
	            "VACANT": "8",
	            "DATE": "28/04/2021",
	            "TIME": "6:15:04 PM",
	            "TYPE": "Private"
        	}
		]
		return dummy_data

	def get_items_from_table(self, tds, s_no, district_name, i, type_hospital):
		
			
		json_obj = {
			"SNO": s_no,
			"DISTRICT": district_name,
			"HOSPITAL_NAME": ".".join(tds[2-i].text.split(".")[1:]).strip(),
			"CONTACT": tds[3-i].text,
			"REGULAR_BEDS_TOTAL": tds[4-i].text,
			"REGULAR_BEDS_OCCUPIED": tds[5-i].text,
			"REGULAR_BEDS_VACANT": tds[6-i].text,
			"OXYGEN_BEDS_TOTAL": tds[7-i].text,
			"OXYGEN_BEDS_OCCUPIED": tds[8-i].text,
			"OXYGEN_BEDS_VACANT": tds[9-i].text,
			"ICU_BEDS_TOTAL": tds[10-i].text,
			"ICU_BEDS_OCCUPIED": tds[11-i].text,
			"ICU_BEDS_VACANT": tds[12-i].text,
			"TOTAL": tds[13-i].text,
			"OCCUPIED": tds[14 -i].text,
			"VACANT": tds[15-i].text,
			"DATE": tds[16-i].text,
			"TIME": tds[17-i].text,
			"TYPE": type_hospital
		}

		return json_obj


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

				# the element for table takes time to load after page is loaded
				time.sleep(4)
				browser.find_elements_by_css_selector("table.table-responsive1 > tbody > tr > td")[1].click()

				all_table_rows = browser.find_elements_by_css_selector("table.table-responsive1 > tbody > tr")

				district_name, s_no = "", 0

				output_json = []
				for table_row in all_table_rows:
					tds = table_row.find_elements_by_css_selector('td')
					if len(tds) == 18:
						s_no = tds[0].text
						district_name = tds[1].text
						i = 0
					else:
						i = 2

					output_json.append(self.get_items_from_table(tds, s_no, district_name, i, "Government"))


				## private hospital tab switching

				browser.find_element_by_css_selector("input[value='P']").click()
				button_elements = browser.find_elements_by_css_selector("button[type='submit']")
				for button_element in button_elements:
					if button_element.text=="VIEW REPORT":
						button_element.click()
						break


				time.sleep(20)

				all_table_rows = browser.find_elements_by_css_selector("table.table-responsive1 > tbody > tr")

				district_name, s_no = "", 0
				for table_row in all_table_rows:
					tds = table_row.find_elements_by_css_selector('td')
					if len(tds) == 18:
						s_no = tds[0].text
						district_name = tds[1].text
						i = 0
					else:
						i = 2

					output_json.append(self.get_items_from_table(tds, s_no, district_name, i, "Private"))

				page_retrieved = True
			except Exception as e:
				print(e)
				print("Page failed to load. Retrying")
				retries +=1
				time.sleep(10)

		if retries >=5:
			return []


		return output_json




	def tag_critical_care(self, merged_loc_df):
		merged_loc_df["HAS_ICU_BEDS"] = merged_loc_df.apply(lambda row: True 
												if int(row["ICU_BEDS_TOTAL"]) > 0 else False, axis=1)

		
		return merged_loc_df

