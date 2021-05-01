import urllib
import json
import requests
import pandas as pd
import logging
import uuid
import sys
import os
import datetime


logging.basicConfig(filename='DataService.log', filemode='a', format='%(asctime)-15s %(message)s', level=logging.DEBUG)

class State(object):

	def __init__(self):
		self.unique_columns = ["DISTRICT", "HOSPITAL_NAME"]
		self.old_info_columns = ["LOCATION", "LAT", "LONG"]
		# To delete all records
		self.delete_condition = {"condition": {}, "limit": 50}

	# def __new__(cls, state_name):
	# 	if(state_name == "TamilNadu"):
	# 		return super().__new__(TamilNadu)
	# 	else:
	# 		raise Exception("State {} not supported".format(state_name))

	def get_location_from_master(self, govt_data_df, sheet_data_df):

		for unique_column in self.unique_columns:
			sheet_data_df[unique_column] = sheet_data_df[unique_column].str.strip()
			govt_data_df[unique_column] = govt_data_df[unique_column].str.strip()

		sheet_data_df_subset = sheet_data_df[self.unique_columns + self.old_info_columns + ["UID", "IS_NEW_HOSPITAL"]]

		bool_values = {"TRUE":True, "FALSE": False}
		sheet_data_df_subset["IS_NEW_HOSPITAL"] = sheet_data_df_subset["IS_NEW_HOSPITAL"].map(bool_values)

		merged_loc_df = pd.merge(govt_data_df, sheet_data_df_subset, 
									on=self.unique_columns, how="left")
		merged_loc_df["IS_NEW_HOSPITAL"] = merged_loc_df["IS_NEW_HOSPITAL"].fillna(value=True)

		merged_loc_df["UID"] = merged_loc_df.apply(lambda row: row["UID"] if (isinstance(row["UID"], str) and 
													row["UID"]!="") else str(uuid.uuid4()), axis=1)

		merged_loc_df = self.tag_critical_care(merged_loc_df)
		merged_loc_df = merged_loc_df.fillna('')
		merged_loc_df["STEIN_ID"] = self.state_name
		merged_loc_df["LAST_SYNCED"] = pd.to_datetime('now').replace(microsecond=0) + pd.Timedelta('05:30:00')
		merged_loc_df["LAST_SYNCED"] = merged_loc_df["LAST_SYNCED"].astype(str)

		merged_loc_df = merged_loc_df.drop_duplicates()
		return merged_loc_df.to_dict('records')


	def push_data(self):

		logging.info("Pushing for state - "+ self.state_name)

		logging.info("Fetching data from source")
		govt_data_df = self.get_data_from_source()

		if len(govt_data_df) > 0:
			self.get_error_message(self.sheet_response)

			sheet_data_df = pd.DataFrame(self.sheet_response)
			# data = self.get_dummy_data()
			logging.info("Fetching location from master sheet")
			location_tagged_data = self.get_location_from_master(govt_data_df, sheet_data_df)

			if len(sheet_data_df)*.9 > len(location_tagged_data):
				logging.info("Row count with the scraped data is low, can cause data loss, Omitting writing to main file")
			else:
				self.write_temp_file(sheet_data_df)

				delete_data_response = self.delete_data_from_sheets()

				if not "error" in delete_data_response:
					self.push_data_to_sheets(location_tagged_data)
					# if not "error" in x.json():
					# 	logging.info("Removing temporary file")
					# 	os.remove(temp_file_name)
		else:
			logging.info("No data retrieved from url")

	def push_data_to_sheets(self, data_json, n=None):
		logging.info("Posting data to Google Sheets")
		if n:
			nested_data = [data_json[i * n:(i + 1) * n] for i in range((len(data_json) + n - 1) // n )]
			for each_data_point in nested_data:
				logging.info("Pushing 50 data points")
				x = requests.post(self.sheet_url, json = each_data_point)
				logging.info(x.text)
		else:
			x = requests.post(self.sheet_url, json = data_json)
			logging.info(x.text)

	def delete_data_from_sheets(self):
		logging.info("Deleting data from {}".format(self.sheet_url))

		number_of_loops = int(self.number_of_records / 50) + 1
		for i in range(0, number_of_loops):
			self.delete_condition["condition"] = {"STEIN_ID": self.state_name}
			response = requests.delete(self.sheet_url, data=json.dumps(self.delete_condition)).json()
			logging.info(response)
		return response

	def write_temp_file(self, data_df):
		temp_file_name = "tmp_{}".format(self.state_name)
		if data_df.empty:
			data_df = pd.read_csv(temp_file_name)
		self.write_data_in_csv(data_df)

	def write_data_in_csv(self, df):
		temp_file_name = "tmp_{}".format(self.state_name)
		logging.info("Creating temp file {}".format(temp_file_name))
		df.to_csv(temp_file_name, index=None)

	def get_non_empty_dict_value(self, key, dict_obj):
		if key in dict_obj:
			return dict_obj[key]
		else:
			return ""

	def get_error_message(self, response):

		if "error" in response:
			print("Error executing API - {}".format(response["error"]))
