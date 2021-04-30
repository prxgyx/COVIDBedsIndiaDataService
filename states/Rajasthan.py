import urllib3
import time
import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from states.State import State
from urllib.request import urlopen
import json
import logging

class Rajasthan(State):

	def __init__(self):
		super().__init__()
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/608983ed03eef39bb4d05a77"
		self.main_sheet_name = "Rajasthan"
		self.state_name = "Rajasthan"
		self.source_url = "https://covidinfo.rajasthan.gov.in/Covid-19hospital-wisebedposition-wholeRajasthan.aspx"
		self.sheet_url = self.stein_url + "/" + self.main_sheet_name
		# Fetching it here because need number of records in the Class
		# need number of records because bulk delete API throws error entity too large
		logging.info("Fetching data from Google Sheets")
		self.sheet_response = requests.get(self.sheet_url).json()
		self.number_of_records = len(self.sheet_response)
		logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))

	def get_data_from_source(self):

		response = requests.get(self.source_url)
		soup = BeautifulSoup(response.text, 'html.parser')

		tables = soup.find_all("tbody")
		storeValueRows = tables[0].find_all("tr")

		storeMatrix = []
		for row in storeValueRows:
		#     print(row)
			storeMatrixRow = []
			for cell in row.find_all("td"):
				storeMatrixRow.append(cell.get_text().strip())
			storeMatrix.append(storeMatrixRow)
		storeMatrix=pd.DataFrame(storeMatrix)


		storeMatrix.columns=['sno','DISTRICT','HOSPITAL_NAME','GENERAL_BEDS_TOTAL','GENERAL_BEDS_OCCUPIED','GENERAL_BEDS_AVAILABLE',
							 'OXYGEN_BEDS_TOTAL','OXYGEN_BEDS_OCCUPIED','OXYGEN_BEDS_AVAILABLE','ICU_BEDS_TOTAL','ICU_BEDS_OCCUPIED',
							 'ICU_BEDS_AVAILABLE','VENTILATOR_BEDS_TOTAL','VENTILATOR_BEDS_OCCUPIED','VENTILATOR_BEDS_AVAILABLE',
							 'HOSPITAL_HELPLINE_NO','DISTRICT_CONTROL_ROOM']

		storeMatrix=storeMatrix.drop(0,axis=0)
		storeMatrix=storeMatrix.drop('sno',axis=1)
		storeMatrix['LAST_UPDATED']=self.lastupdate()
		
		return storeMatrix

	def lastupdate(self):
		now = datetime.now()
		if (int(now.strftime('%H')) in ([20,21,22,23])):
			UpdateHour='Last Update: Today 8 PM'
		elif (int(now.strftime('%H')) in ([0,1,2,3,4,5,6,7,8])):
			UpdateHour='Last Update: Yesterday 8 PM'
		elif(int(now.strftime('%H')) in ([9,10,11,12,13,14])):
			UpdateHour='Last Update: Today 9 AM'
		elif(int(now.strftime('%H')) in ([15,16,17,18,19])):
			UpdateHour='Last Update: Today 2 PM'
		return(UpdateHour)

	def tag_critical_care(self, merged_loc_df):
		logging.info("Tagged critical care")
		merged_loc_df["HAS_ICU_BEDS"] = merged_loc_df.apply(lambda row: int(row["ICU_BEDS_TOTAL"]) > 0, axis=1)
		merged_loc_df["HAS_VENTILATORS"] = merged_loc_df.apply(lambda row: int(row["VENTILATOR_BEDS_TOTAL"]) > 0, axis=1)
		return merged_loc_df