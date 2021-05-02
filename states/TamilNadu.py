import urllib3
from bs4 import BeautifulSoup
import requests
from states.State import State
import logging
import pandas as pd

class TamilNadu(State):

	def __init__(self, test_prefix=None):
		super().__init__()
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/608970d903eef3cbe0d05a6b"
		self.source_url = "https://stopcorona.tn.gov.in/beds.php"
		self.main_sheet_name = "Tamil Nadu"
		if test_prefix:
			self.main_sheet_name = test_prefix + self.main_sheet_name
		self.state_name = "Tamil Nadu"
		self.sheet_url = self.stein_url + "/" + self.main_sheet_name
		# Fetching it here because need number of records in the Class
		# need number of records because bulk delete API throws error entity too large
		logging.info("Fetching data from Google Sheets")
		self.sheet_response = requests.get(self.sheet_url).json()
		self.number_of_records = len(self.sheet_response)
		logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))

	def get_data_from_source(self):
		http = urllib3.PoolManager()
		
		output_json = []

		s_no = 0

		response = http.request('GET', self.source_url)
		soup = BeautifulSoup(response.data, "html.parser")
		json_obj = 0
		for tr in soup.find_all('tr')[2:]:
			tds = tr.find_all('td')
			json_obj = {
			"SNO": s_no +1 ,
			"DISTRICT": tds[0].text,
			"HOSPITAL_NAME": tds[1].text,
			"BEDS_FOR_SUSPECTED_CASES_TOTAL": tds[2].text,
			"BEDS_FOR_SUSPECTED_CASES_OCCUPIED": tds[3].text,
			"BEDS_FOR_SUSPECTED_CASES_VACANT": tds[4].text,
			"OXYGEN_SUPPORTED_BEDS_TOTAL": tds[5].text,
			"OXYGEN_SUPPORTED_BEDS_OCCUPIED": tds[6].text,
			"OXYGEN_SUPPORTED_BEDS_VACANT": tds[7].text,
			"NONOXYGEN_SUPPORTED_BEDS_TOTAL": tds[8].text,
			"NONOXYGEN_SUPPORTED_BEDS_OCCUPIED": tds[9].text,
			"NONOXYGEN_SUPPORTED_BEDS_VACANT": tds[10].text,
			"ICU_BEDS_TOTAL": tds[11].text,
			"ICU_BEDS_OCCUPIED": tds[12].text,
			"ICU_BEDS_VACANT": tds[13].text,
			"VENTILATOR_TOTAL": tds[14].text,
			"VENTILATOR_OCCUPIED": tds[15].text,
			"VENTILATOR_VACANT": tds[16].text,
			"LAST_UPDATED": tds[17].text,
			"CONTACT": tds[18].text,
			"REMARKS": tds[19].text
			}
			s_no = s_no + 1
			output_json.append(json_obj)
		return pd.DataFrame(output_json)

	def tag_critical_care(self, merged_loc_df):
		logging.info("Tagged critical care")
		merged_loc_df["HAS_ICU_BEDS"] = merged_loc_df.apply(lambda row: int(row["ICU_BEDS_TOTAL"]) > 0, axis=1)
		merged_loc_df["HAS_VENTILATORS"] = merged_loc_df.apply(lambda row: int(row["VENTILATOR_TOTAL"]) > 0, axis=1)
		return merged_loc_df
