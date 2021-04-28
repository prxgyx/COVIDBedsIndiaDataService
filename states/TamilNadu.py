import urllib3
from bs4 import BeautifulSoup
import requests
from states.State import State

class TamilNadu(State):

	def __init__(self):
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/608970d903eef3cbe0d05a6b"
		self.source_url = "https://stopcorona.tn.gov.in/beds.php"
		self.custom_sheet_name = "Sheet13"
		self.main_sheet_name = "Tamil Nadu"

	def get_dummy_data(self):
		dummy_data = [
			{
				"SNO":2,
				"DISTRICT":"Chennai",
				"HOSPITAL_NAME":"Bharathiraja Hospital & Research Centre Pvt Ltd",
				"BEDS_FOR_SUSPECTED_CASES_TOTAL":"25",
				"BEDS_FOR_SUSPECTED_CASES_OCCUPIED":"17",
				"BEDS_FOR_SUSPECTED_CASES_VACANT":"8",
				"OXYGEN_SUPPORTED_BEDS_TOTAL":"8",
				"OXYGEN_SUPPORTED_BEDS_OCCUPIED":"6",
				"OXYGEN_SUPPORTED_BEDS_VACANT":"2",
				"NONOXYGEN_SUPPORTED_BEDS_TOTAL":"12",
				"NONOXYGEN_SUPPORTED_BEDS_OCCUPIED":"11",
				"NONOXYGEN_SUPPORTED_BEDS_VACANT":"1",
				"ICU_BEDS_TOTAL":"5",
				"ICU_BEDS_OCCUPIED":"0",
				"ICU_BEDS_VACANT":"5",
				"VENTILATOR_TOTAL":"2",
				"VENTILATOR_OCCUPIED":"0",
				"VENTILATOR_VACANT":"2",
				"DATETIME":"2021-04-28 10:22:10",
				"CONTACT":"9791295212",
				"REMARKS":"28.04.2021"
			}
		]
		return dummy_data

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
			"DATETIME": tds[17].text,
			"CONTACT": tds[18].text,
			"REMARKS": tds[19].text
			}
			s_no = s_no + 1
			output_json.append(json_obj)
		return output_json

	def tag_critical_care(self, hsp_info):
		hsp_info["HAS_ICU_BEDS"] = int(hsp_info["ICU_BEDS_TOTAL"]) > 0
		hsp_info["HAS_VENTILATORS"] = int(hsp_info["VENTILATOR_TOTAL"]) > 0
