import urllib3
from bs4 import BeautifulSoup
import requests
from states.FreshState import FreshState
import time
from urllib.request import urlopen
import pandas as pd
import json
import logging
import uuid
import datetime
import re

class Haryana(FreshState):

	def __init__(self, test_prefix=None):
		self.state_name = "Haryana"
		super().__init__()
		self.distL={"Ambala":1,"Bhiwani":2,"Chandigarh":24,"Charki Dadri":3,"Faridabad":4,"Fatehabad":5,"Gurugram":6,"Hisar":7,"Jhajjar":8,"Jind":9,"Kaithal":10,"Karnal":11,"Kurukshetra":12,"Mahendragarh":13,"Nuh":23,"Palwal":15,"Panchkula":16,"Panipat":17,"Rewari":18,"Rohtak":19,"Sirsa":20,"Sonipat":21,"Yamunanagar":22}
		self.main_sheet_name = "Haryana"
		if test_prefix:
			self.main_sheet_name = test_prefix + self.main_sheet_name
		self.sheet_url = self.stein_url + "/" + self.main_sheet_name
		# Fetching it here because need number of records in the Class
		# need number of records because bulk delete API throws error entity too large
		logging.info("Fetching data from Google Sheets")
		self.sheet_response = requests.get(self.sheet_url).json()
		self.number_of_records = len(self.sheet_response)
		logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))
		self.unique_columns = ["FACILITY_NAME", "CITY"]

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
		length_cols = len(locationsplit.columns)
		locationsplit.columns= ["col" + str(i) for i in range(1,length_cols+1)]

		locationsplit['LAT'] = locationsplit['col1'].astype(str).str.strip('showLocation(')
		locationsplit['LAT'] = locationsplit['LAT'].replace("\'", "", regex=True)
		locationsplit['LONG'] = locationsplit['col2'].replace("\'", "", regex=True)

		finaldata=pd.concat([finaldata[['HOSPITAL_INFO','LAST_UPDATED','CITY']],locationsplit[['LAT','LONG']]],axis=1)

		finaldata["STEIN_ID"] = self.state_name

		finaldata["attr_list"] = finaldata.apply(lambda row: self.split_hsp_info_into_att_list(row.HOSPITAL_INFO), axis=1)

		attr_key_value = {"FACILITY_NAME": "Facility Name"}
		# "AVAILABILITY_OF_BEDS": "Availability of Beds", 
		# "OXYGEN_BEDS": " Oxygen Beds", 
		# "NON-OXYGEN_BEDS": "Non-Oxygen Beds", 
		# "ICU_BEDS": "ICU Beds", 
		# "VENTILATORS": "Ventilators", 
		# "AVAILABILITY_OF_OXYGEN": "Availability of Oxygen"}

		for (col,col_name) in attr_key_value.items():
			finaldata[col] = finaldata.apply(lambda row: self.get_attr_value(row.attr_list, col_name), axis=1)

		df = self.join_master_df(finaldata)

		output_json = json.loads(df.to_json(orient="records"))

		return output_json


	def split_hsp_info_into_att_list(self, hsp_info):
		return re.split(', |\n', hsp_info)

	def get_attr_value(self, attr_list, attr):
		subset_list = [x for x in attr_list if attr in x]
		length_subset = len(subset_list)
		if length_subset > 1:
			raise Exception("Error for attribute {} in {}".format(attr, attr_list))
		if length_subset == 0:
			return ""
		else:
			attr_info = subset_list[0]
			attr_value = attr_info.replace(attr, "").replace(",", "").replace(":", "").strip()
			return attr_value

	def get_key(self, val):
		for key, value in self.distL.items():
			if val == value:
				return key
		return "key doesn't exist"
