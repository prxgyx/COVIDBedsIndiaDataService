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





class WestBengal(State):

	def __init__(self, test_prefix=None):
		self.state_name = "West Bengal"
		super().__init__()
		self.source_url = "https://excise.wb.gov.in/CHMS/Public/Page/CHMS_Public_Hospital_Bed_Availability.aspx"
		self.main_sheet_name = "West Bengal"
		if test_prefix:
			self.main_sheet_name = test_prefix + self.main_sheet_name
		self.sheet_url = self.stein_url + "/" + self.main_sheet_name
		# Fetching it here because need number of records in the Class
		# need number of records because bulk delete API throws error entity too large
		logging.info("Fetching data from Google Sheets")
		self.sheet_response = requests.get(self.sheet_url).json()
		self.number_of_records = len(self.sheet_response)
		logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))
		self.icu_beds_column = "CCU_BEDS_WITHOUT_VENTILATOR_TOTAL"
		self.vent_beds_column = "CCU_BEDS_WITH_VENTILATOR_TOTAL"


	def extract_vacant_filled_beds(self, element):
		vacant_filled_info = element.find_elements_by_css_selector('ul > li > h3')
		return vacant_filled_info[0].text, vacant_filled_info[1].text


	def extract_data_from_card(self, tr_card, hosp_type, district):
		json_obj = {"DISTRICT":district, "TYPE":hosp_type}
		json_obj["HOSPITAL_NAME"] = tr_card.find_element_by_css_selector("div[class='card-header'] > h5").text.split("(")[0].strip()
		address_contact = tr_card.find_elements_by_css_selector("div[class='card-header'] > div.row > div.card-text")
		for div_tag in address_contact[:2]:
			if div_tag.text.strip().startswith("Tap to Call"):
				json_obj["CONTACT"] = div_tag.find_element_by_css_selector("a").get_attribute("href").split(":")[-1]
			else:
				json_obj["ADDRESS"] = div_tag.text.strip()

		json_obj["LAST_UPDATED"] = ":".join(tr_card.find_element_by_css_selector("div[class='card-footer text-muted ']").text.split(":")[1:]).strip()
		card_data = tr_card.find_elements_by_css_selector("div[class='card-body'] > div[class='form-group row']")
		total_beds_card = card_data[0].find_elements_by_css_selector('div[class="col mb-4 text-center"]')[-1]
		json_obj["TOTAL"], json_obj["VACANT"] = self.extract_vacant_filled_beds(total_beds_card)
		ind_bed_info = card_data[1].find_elements_by_css_selector('div[class="form-group row"] > div[class="col-lg-6 col-md-6 col-sm-12 mb-4"]')
		json_obj["COVID_BEDS_REGULAR_TOTAL"], json_obj["COVID_BEDS_REGULAR_VACANT"] = self.extract_vacant_filled_beds(ind_bed_info[0])
		json_obj["COVID_BEDS_WITH_OXYGEN_TOTAL"], json_obj["COVID_BEDS_WITH_OXYGEN_VACANT"] = self.extract_vacant_filled_beds(ind_bed_info[1])
		json_obj["HDU_BEDS_TOTAL"], json_obj["HDU_BEDS_VACANT"] = self.extract_vacant_filled_beds(ind_bed_info[2])
		json_obj["CCU_BEDS_WITHOUT_VENTILATOR_TOTAL"], json_obj["CCU_BEDS_WITHOUT_VENTILATOR_VACANT"] = self.extract_vacant_filled_beds(ind_bed_info[3])
		json_obj["CCU_BEDS_WITH_VENTILATOR_TOTAL"], json_obj["CCU_BEDS_WITH_VENTILATOR_VACANT"] = self.extract_vacant_filled_beds(ind_bed_info[4])

		if json_obj["HDU_BEDS_TOTAL"]=="":
			print("Error will retry for")
			json_obj
			return{}
		print(json_obj)
		return json_obj


	def wait_driver(self, browser, css_selector, time=10):
		elements = WebDriverWait(browser, time).until( \
	        EC.visibility_of_all_elements_located((By.CSS_SELECTOR,css_selector)))
		return elements


	def get_data_from_source(self):
		fireFoxOptions = webdriver.FirefoxOptions()
		# fireFoxOptions.set_headless()
		browser = webdriver.Firefox(firefox_options=fireFoxOptions)

		browser.get(self.source_url)

		element_dropdown = WebDriverWait(browser, 10).until(
		        EC.presence_of_element_located((By.CSS_SELECTOR, "select[class='form-control']"))
		    )
		time.sleep(1)
		element_dropdown.click()

		all_districts_list = list(range(1, len(browser.find_elements_by_css_selector("select[class='form-control'] > option")[1:])+1))
		print(all_districts_list)

		prev_hosp_name, prev_hosp_count = "", 0

		retry_counter = {}

		output_json = []

		while len(all_districts_list) > 0:
			curr_element = all_districts_list.pop(0)
			if curr_element in retry_counter:
				retry_counter[curr_element]+=1
			else:
				retry_counter[curr_element] = 0

			district = self.wait_driver(browser, "select[class='form-control'] > option")[curr_element]
			try:
				district_name = district.text.strip()
				district.click()
				time.sleep(2)
				list_control = self.wait_driver(browser, "span[class='ListControl'] > label")[:3]
				hosp_type = list_control[0].text.strip()

				for i in range(len(list_control)):
					label_element = self.wait_driver(browser, "span[class='ListControl'] > label")[i]
					hosp_type = label_element.text
					label_element.click()

					time.sleep(2)
					page_not_loaded, retries = True, 0

					while page_not_loaded and retries<2:
						try:
							all_table_rows = self.wait_driver(browser, 'table > tbody > tr')
							if len(all_table_rows)>10:
								if all_table_rows[2].find_element_by_css_selector(
													"div[class='card-header'] > h5").text.split("(")[0].strip()!=prev_hosp_name:
									page_not_loaded = False
								else:
									time.sleep(5)
							elif len(all_table_rows)>0 and len(all_table_rows)<=10:
								if all_table_rows[0].find_element_by_css_selector(
													"div[class='card-header'] > h5").text.split("(")[0].strip()!=prev_hosp_name:
									page_not_loaded = False
								else:
									time.sleep(5)

						except Exception as e:
							if prev_hosp_count > 0:
								page_not_loaded = False
							all_table_rows=[]
							retries+=1
							print(e)
							
					prev_hosp_count = len(all_table_rows)

					if len(all_table_rows)>10:
						len_pages = int(all_table_rows	[0].find_elements_by_css_selector('tr > td > a')[-1].text)
						for j in range(1, len_pages+1):
							details = self.wait_driver(browser, "a[class='btn btn-link']")[0]
							details.click()
							time.sleep(3)

							tr_cards = self.wait_driver(browser, 'table > tbody > tr')[2:-2]				
							for k in range(len(tr_cards)):
								json_data = self.extract_data_from_card(tr_cards[k], hosp_type, district_name)
								if "HOSPITAL_NAME" in json_data:
									if k==0:
										prev_hosp_name = json_data["HOSPITAL_NAME"]
									output_json.append(json_data)

							
							pages_tr = self.wait_driver(browser, "tr[class='pagination-ys']")[0]
							pages = self.wait_driver(pages_tr, 'tr > td > a')
							for page in pages:
								if page.text.strip()==str(j+1):
									page.click()
									time.sleep(2)
									page_not_updated = True
									while page_not_updated:
										try:
											if self.wait_driver(browser, 'table > tbody > tr')[2].find_element_by_css_selector(
														"div[class='card-header'] > h5").text.split("(")[0].strip()!=prev_hosp_name:
												page_not_updated = False
										except:
											time.sleep(2)
									
									break

					elif len(all_table_rows)>0 and len(all_table_rows)<=10:
						details = self.wait_driver(browser, "a[class='btn btn-link']")[0]
						details.click()
						time.sleep(3)

						tr_cards = self.wait_driver(browser, 'table > tbody > tr')			
						for k in range(len(tr_cards)):
							json_data = self.extract_data_from_card(tr_cards[k], hosp_type, district_name)
							if "HOSPITAL_NAME" in json_data:
								if k==0:
									prev_hosp_name = json_data["HOSPITAL_NAME"]
								output_json.append(json_data)
			except Exception as e:
				print(e)
				if retry_counter[curr_element]<3:
					all_districts_list.append(curr_element)
			time.sleep(5)

		browser.close()
		browser.quit()
		
		return pd.DataFrame(output_json)




