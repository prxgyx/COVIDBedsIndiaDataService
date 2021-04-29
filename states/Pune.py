import urllib3
import urllib
import json
from states.State import State
import requests

class Pune(State):

	def __init__(self):
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/6089822703eef30c1cd05a6e"
		self.source_url = "https://covidpune.com/data/covidpune.com/bed_data.json?_=7528f9d_20210426225550"
		self.custom_sheet_name = "Sheet4"
		self.main_sheet_name = "Pune"

	def get_dummy_data(self):
		dummy_data = [
			{
				"DISTRICT": "Pune",
				"AREA": "PCMC",
				"HOSPITAL_CATEGORY": "DCHC",
				"HOSPITAL_NAME": "Chaudhari Hopsital, Chikhali",
				"HOSPITAL_ADDRESS": "Moshi - Alandi Rd, opp. Indian CNG petrol pump infront of madhubhan lodge, Jadhav Wadi, Chikhali, chikali, Maharashtra 412114",
				"CONTACT": "8237770911",
				"LAST_UPDATED": "1619607156000",
				"OFFICER_NAME": "No Data Available",
				"OFFICER_DESIGNATION": "No Data Available",
				"CHARGES": "Chargeable",
				"FEE_REGULATED_BEDS": "0",
				"TOTAL_BEDS_ALLOCATED_TO_COVID": "23",
				"TOTAL_BEDS_WITHOUT_OXYGEN": "5",
				"AVAILABLE_BEDS_WITHOUT_OXYGEN": "3",
				"TOTAL_BEDS_WITH_OXYGEN": "11",
				"AVAILABLE_BEDS_WITH_OXYGEN": "2",
				"TOTAL_ICU_BEDS_WITHOUT_VENTILATOR": "6",
				"AVAILABLE_ICU_BEDS_WITHOUT_VENTILATOR": "0",
				"TOTAL_ICU_BEDS_WITH_VENTILATOR": "1",
				"AVAILABLE_ICU_BEDS_WITH_VENTILATOR": "0"
			}
		]
		return dummy_data

	def get_data_from_source(self):
		http = urllib3.PoolManager()
		
		output_json = []

		s_no = 0

		response = urllib.request.urlopen(self.source_url)
		data = json.loads(response.read().decode())
		# with open("x.json", 'w') as outfile:
		# 	outfile.write(data)
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
		return output_json

	def tag_critical_care(self, hsp_info):
		hsp_info["HAS_ICU_BEDS"] = int(hsp_info["TOTAL_ICU_BEDS_WITHOUT_VENTILATOR"]) > 0
		hsp_info["HAS_VENTILATORS"] = int(hsp_info["TOTAL_ICU_BEDS_WITH_VENTILATOR"]) > 0

