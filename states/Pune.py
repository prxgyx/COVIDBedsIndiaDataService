import urllib3
import urllib
import json
from states.State import State
import requests
import logging
import pandas as pd

class Pune(State):

	def __init__(self):
		super().__init__()
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/6089822703eef30c1cd05a6e"
		self.source_url = "https://covidpune.com/data/covidpune.com/bed_data.json?_=7528f9d_20210426225550"
		self.main_sheet_name = "Pune"
		self.state_name = "Pune"
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

		response = urllib.request.urlopen(self.source_url)
		data = json.loads(response.read().decode())
		
		for each_data_point in data:
			json_obj = {
				"DISTRICT": each_data_point["district"],
				"AREA": each_data_point["area"],
				"HOSPITAL_CATEGORY": each_data_point["hospital_category"],
				"HOSPITAL_NAME": each_data_point["hospital_name"],
				"HOSPITAL_ADDRESS": each_data_point["hospital_address"],
				"CONTACT": each_data_point["hospital_phone"],
				"LAST_UPDATED": each_data_point["last_updated_on"],
				"OFFICER_NAME": each_data_point["officer_name"],
				"OFFICER_DESIGNATION": each_data_point["officer_designation"],
				"CHARGES": each_data_point["charges"],
				"FEE_REGULATED_BEDS": each_data_point["fee_regulated_beds"],
				"TOTAL_BEDS_ALLOCATED_TO_COVID": each_data_point["total_beds_allocated_to_covid"],
				"TOTAL_BEDS_WITHOUT_OXYGEN": each_data_point["total_beds_without_oxygen"],
				"AVAILABLE_BEDS_WITHOUT_OXYGEN": each_data_point["available_beds_without_oxygen"],
				"TOTAL_BEDS_WITH_OXYGEN": each_data_point["total_beds_with_oxygen"],
				"AVAILABLE_BEDS_WITH_OXYGEN": each_data_point["available_beds_with_oxygen"],
				"TOTAL_ICU_BEDS_WITHOUT_VENTILATOR": each_data_point["total_icu_beds_without_ventilator"],
				"AVAILABLE_ICU_BEDS_WITHOUT_VENTILATOR": each_data_point["available_icu_beds_without_ventilator"],
				"TOTAL_ICU_BEDS_WITH_VENTILATOR": each_data_point["total_icu_beds_with_ventilator"],
				"AVAILABLE_ICU_BEDS_WITH_VENTILATOR": each_data_point["available_icu_beds_with_ventilator"],
				"PINCODE": self.get_non_empty_dict_value("pincode", each_data_point)
			}
			s_no = s_no + 1
			output_json.append(json_obj)
		return pd.DataFrame(output_json)

	def tag_critical_care(self, merged_loc_df):
		logging.info("Tagged critical care")
		merged_loc_df["HAS_ICU_BEDS"] = merged_loc_df.apply(lambda row: int(row["TOTAL_ICU_BEDS_WITHOUT_VENTILATOR"]) > 0, axis=1)
		merged_loc_df["HAS_VENTILATORS"] = merged_loc_df.apply(lambda row: int(row["TOTAL_ICU_BEDS_WITH_VENTILATOR"]) > 0, axis=1)
		return merged_loc_df

