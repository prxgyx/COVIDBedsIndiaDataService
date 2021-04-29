import urllib3
from bs4 import BeautifulSoup
import requests
from states.State import State
import time
from urllib.request import urlopen
import pandas as pd
import json

class Haryana(State):

	def __init__(self):
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/6089834e03eef33448d05a74"
		self.distL={"Ambala":1,"Bhiwani":2,"Chandigarh":24,"Charki Dadri":3,"Faridabad":4,"Fatehabad":5,"Gurugram":6,"Hisar":7,"Jhajjar":8,"Jind":9,"Kaithal":10,"Karnal":11,"Kurukshetra":12,"Mahendragarh":13,"Nuh":23,"Palwal":15,"Panchkula":16,"Panipat":17,"Rewari":18,"Rohtak":19,"Sirsa":20,"Sonipat":21,"Yamunanagar":22}
		self.custom_sheet_name = "Sheet3"
		self.main_sheet_name = "Haryana"

	def get_data_from_source(self):
		finaldata=pd.DataFrame()
		
		for city in range(1,25):
			try:
				print (city)
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
				print(city , "city not found")

			finaldata=pd.concat([finaldata,deets],axis=0)

		locationsplit=finaldata.location.str.split(',', expand = True)
		locationsplit.columns=['col1','col2','col3','col4','col5']

		locationsplit['LAT'] = locationsplit['col1'].astype(str).str.strip('showLocation(')
		locationsplit['LAT'] = locationsplit['LAT'].replace("\'", "", regex=True)
		locationsplit['LONG'] = locationsplit['col2'].replace("\'", "", regex=True)

		finaldata=pd.concat([finaldata[['HOSPITAL_INFO','LAST_UPDATED','CITY']],locationsplit[['LAT','LONG']]],axis=1)
		
		output_json = json.loads(finaldata.to_json(orient="records"))

		return output_json

		# with open('data.txt', 'w') as outfile:
		# 	json.dump(output_json, outfile)

	def get_key(self, val):
		for key, value in self.distL.items():
			if val == value:
				return key
		return "key doesn't exist"

	def push_data(self):

		url = self.stein_url + "/" + self.custom_sheet_name
		n = 50

		print("Fetching data from source")
		data = self.get_data_from_source()
		# data = self.get_dummy_data()

		nested_data = [data[i * n:(i + 1) * n] for i in range((len(data) + n - 1) // n )]

		print("Posting data to Google Sheets")
		
		for each_data_point in nested_data:
			print("Pushing 50 data points")
			x = requests.post(url, json = each_data_point)
			print(x.text)
