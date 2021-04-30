import urllib
import json
import requests
import pandas as pd
import logging
import uuid
import sys
import os

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
		merged_loc_df = pd.merge(govt_data_df, sheet_data_df_subset, 
									on=self.unique_columns, how="left")
		merged_loc_df["IS_NEW_HOSPITAL"] = merged_loc_df.apply(lambda row: True
												 if isinstance(row["IS_NEW_HOSPITAL"], float) else row["IS_NEW_HOSPITAL"], axis=1)

		merged_loc_df["UID"] = merged_loc_df.apply(lambda row: str(uuid.uuid4()) if row["IS_NEW_HOSPITAL"] else row["UID"], axis=1)

		merged_loc_df = self.tag_critical_care(merged_loc_df)
		merged_loc_df = merged_loc_df.fillna('')
		merged_loc_df["STEIN_ID"] = self.state_name
		return merged_loc_df.to_dict('records')

	def push_data(self):

		logging.info("Pushing for state - "+ self.state_name)

		n = 50

		logging.info("Fetching data from source")
		govt_data_df = self.get_data_from_source()

		self.get_error_message(self.sheet_response)

		sheet_data_df = pd.DataFrame(self.sheet_response)

		temp_file_name = "tmp_{}".format(self.state_name)

		if sheet_data_df.empty:
			sheet_data_df = pd.read_csv(temp_file_name)

		# data = self.get_dummy_data()
		logging.info("Fetching location from master sheet")
		location_tagged_data = self.get_location_from_master(govt_data_df, sheet_data_df)

		nested_data = [location_tagged_data[i * n:(i + 1) * n] for i in range((len(location_tagged_data) + n - 1) // n )]

		self.write_data_in_csv(sheet_data_df)
		delete_data_response = self.delete_data()

		if not "error" in delete_data_response:
			logging.info("Posting data to Google Sheets")
			for each_data_point in nested_data:
				logging.info("Pushing 50 data points")
				x = requests.post(self.sheet_url, json = each_data_point)
				logging.info(x.text)
			# if not "error" in x.json():
			# 	logging.info("Removing temporary file")
			# 	os.remove(temp_file_name)

	def delete_data(self):
		logging.info("Deleting data from {}".format(self.sheet_url))

		number_of_loops = int(self.number_of_records / 50) + 1
		for i in range(0, number_of_loops):
			self.delete_condition["condition"] = {"STEIN_ID": self.state_name}
			response = requests.delete(self.sheet_url, data=json.dumps(self.delete_condition)).json()
			logging.info(response)
		return response

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
