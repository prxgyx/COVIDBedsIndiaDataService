import os
import selenium
from selenium import webdriver
from states.FreshState import FreshState
import time
import io
import requests
import math
from selenium.webdriver.common.keys import Keys
import logging
import pandas as pd
from states.notification.TelegramBot import TelegramBot

covidbedsbot = TelegramBot()

class Bihar(FreshState):

	def __init__(self, test_prefix=None):
		super().__init__()
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/60a62cf2e75f9c105696eb38"
		self.source_url = "https://covid19health.bihar.gov.in/DailyDashboard/BedsOccupied"
		self.main_sheet_name = "Bihar"
		if test_prefix:
			self.main_sheet_name = test_prefix + self.main_sheet_name
		self.state_name = "Bihar"
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

		time.sleep(3)

		# Clear default value Patna
		browser.find_elements_by_xpath("//input[@class='form-control form-control-sm']")[0].send_keys(Keys.BACK_SPACE*5)

		time.sleep(3)

		# select 100 values in 1 page
		browser.find_elements_by_xpath("//select[@class='custom-select custom-select-sm form-control form-control-sm']/option[text()='100']")[0].click()

		time.sleep(5)
		num_entries = int(browser.find_elements_by_xpath("//div[@id='example_info']")[0].text.split('of ')[1].split(' ')[0])

		num_iter = math.ceil(num_entries/100)

		json_list = []

		for i in range(0, num_iter):
			page_num = i+1
			logging.info("Paginated to page number - {}".format(page_num))
			browser.find_elements_by_xpath("//a[@data-dt-idx='{}']".format(page_num))[0].click()
			time.sleep(5)
			self.get_row_list_from_single_page(browser, json_list)
		
		# self.get_row_list_from_single_page(browser, json_list)

		json_df = pd.DataFrame(json_list)
		df = self.join_master_df(json_df)

		return df.to_dict('records')

	def get_row_list_from_single_page(self, browser, json_list):
		all_table_rows = browser.find_elements_by_css_selector("table > tbody > tr")
		for table_row in all_table_rows:
			tds = table_row.find_elements_by_css_selector('td')
			tdlink = table_row.find_elements_by_css_selector('td > span > a')
			loc = tdlink[0].get_attribute('href')
			icu_beds = int(tds[7].text)
			dict_row = {
			'DISTRICT' : tds[0].text,
			'FACILITY_NAME' : tds[1].text,
			'FACILITY_TYPE' : tds[2].text,
			'CATEGORY' : tds[3].text,
			'LAST_UPDATED' : tds[4].text,
			'TOTAL_BEDS' : tds[5].text,
			'VACANT' : tds[6].text,
			'TOTAL_ICU_BEDS' : icu_beds,
			'ICU_BEDS_VACANT' : tds[8].text,
			'CONTACT' : tds[9].text,
			'LAT': loc.split('?')[1].split('=')[1].split('&')[0],
			'LONG': loc.split('?')[1].split('=')[2].split('&')[0],
			'STEIN_ID': self.state_name,
			'HAS_ICU_BEDS': icu_beds > 0
			}
			json_list.append(dict_row)
