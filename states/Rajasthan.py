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
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/608983ed03eef39bb4d05a77"
		self.custom_sheet_name = "Sheet4"
		self.main_sheet_name = "Rajasthan"

	def get_data_from_source(self):

		url='https://covidinfo.rajasthan.gov.in/Covid-19hospital-wisebedposition-wholeRajasthan.aspx'
		response = requests.get(url)
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
		storeMatrix['LAST_UPDATED']=lastupdate()
		
		output_json = json.loads(storeMatrix.to_json(orient="records"))

		return output_json

		# with open('data.txt', 'w') as outfile:
		# 	json.dump(output_json, outfile)

	def lastupdate():
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