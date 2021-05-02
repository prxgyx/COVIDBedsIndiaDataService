import urllib3
from bs4 import BeautifulSoup
import requests
from states.State import State
import time
from urllib.request import urlopen
import pandas as pd
import json
import logging
from states.notification.TelegramBot import TelegramBot
import uuid
import datetime


covidbedsbot = TelegramBot()


class Haryana(State):

	def __init__(self, test_prefix=None):
		super().__init__()
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/6089834e03eef33448d05a74"
		self.distL={"Ambala":1,"Bhiwani":2,"Chandigarh":24,"Charki Dadri":3,"Faridabad":4,"Fatehabad":5,"Gurugram":6,"Hisar":7,"Jhajjar":8,"Jind":9,"Kaithal":10,"Karnal":11,"Kurukshetra":12,"Mahendragarh":13,"Nuh":23,"Palwal":15,"Panchkula":16,"Panipat":17,"Rewari":18,"Rohtak":19,"Sirsa":20,"Sonipat":21,"Yamunanagar":22}
		self.main_sheet_name = "Haryana"
		if test_prefix:
			self.main_sheet_name = test_prefix + self.main_sheet_name
		self.state_name = "Haryana"
		self.sheet_url = self.stein_url + "/" + self.main_sheet_name
		# Fetching it here because need number of records in the Class
		# need number of records because bulk delete API throws error entity too large
		logging.info("Fetching data from Google Sheets")
		self.sheet_response = requests.get(self.sheet_url).json()
		self.number_of_records = len(self.sheet_response)
		logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))

	def get_data_from_source(self):
		finaldata=pd.DataFrame()
		
		for city in range(1,25):
			try:
				logging.info(city)
				url='https://coronaharyana.in/?city='+str(city)
				response = requests.get(url)
				soup = BeautifulSoup(response.text, 'html.parser')
				a = soup.find_all('div', class_='entry-content')
				b = soup.find_all('div', class_='post-meta-wrapper')

				deets=[]
				for i in a:
					# hospitalName=i.find('h6').text
					# contactNo=i.find('span').text
					article_text=''
					article_text += '\n' + ''.join(i.findAll(text = True))
					newrow=[article_text]
					deets.append(newrow)
				deets=pd.DataFrame(deets)
				deets.columns=['HOSPITAL_INFO']

				loca=[]
				lastup=[]
				for j in b:
					location=j.find('a')['onclick']
					lastUpdated=j.text
					loca.append(location)
					lastup.append(lastUpdated)
					
				deets['location']=loca
				deets['LAST_UPDATED']=lastup
				deets['CITY']= self.get_key(city)
			except Exception as e:
				logging.info(city , "city not found")

			finaldata=pd.concat([finaldata,deets],axis=0)

		locationsplit=finaldata.location.str.split(',', expand = True)
		locationsplit.columns=['col1','col2','col3','col4','col5']

		locationsplit['LAT'] = locationsplit['col1'].astype(str).str.strip('showLocation(')
		locationsplit['LAT'] = locationsplit['LAT'].replace("\'", "", regex=True)
		locationsplit['LONG'] = locationsplit['col2'].replace("\'", "", regex=True)

		finaldata=pd.concat([finaldata[['HOSPITAL_INFO','LAST_UPDATED','CITY']],locationsplit[['LAT','LONG']]],axis=1)

		finaldata["STEIN_ID"] = self.state_name
		
		output_json = json.loads(finaldata.to_json(orient="records"))

		return output_json

		# with open('data.txt', 'w') as outfile:
		# 	json.dump(output_json, outfile)


	def add_uid_lastsynced(self, data):
		now  = datetime.datetime.now()
		for hosp_info in data:
			hosp_info["UID"] = str(uuid.uuid4())
			hosp_info["LAST_SYNCED"] = now.strftime("%Y-%m-%d, %H:%M:%S")   
			hosp_info["IS_NEW_HOSPITAL"] = False
		return data 



	def push_data(self):

		url = self.stein_url + "/" + self.main_sheet_name

		logging.info("Fetching data from source")
		# data = self.get_data_from_source()
		import pickle
		with open("haryana.pkl", "rb") as f:
			data = pickle.load(f)
		if len(data) > 0:
			sheet_data_df = pd.DataFrame(self.sheet_response)
			data = self.add_uid_lastsynced(data)

			self.write_temp_file(sheet_data_df)
			delete_data_response = self.delete_data_from_sheets()
			if not "error" in delete_data_response:
				self.push_data_to_sheets(data, 50)
				covidbedsbot.send_message(self.success_msg_info(sheet_data_df, data))
				covidbedsbot.send_local_file("tmp_{}".format(self.state_name))

			else:
				failure_reason = "Error deleting data from sheets"
				covidbedsbot.send_message(self.error_msg_info(failure_reason, sheet_data_df))
		else:
			failure_reason = "No data retrieved from url"
			logging.info(failure_reason)
			covidbedsbot.send_message(self.error_msg_info(failure_reason, sheet_data_df))



	def get_key(self, val):
		for key, value in self.distL.items():
			if val == value:
				return key
		return "key doesn't exist"
