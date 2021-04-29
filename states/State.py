import urllib
import json
import requests
import pandas as pd
import logging
import uuid

logging.basicConfig(filename='DataService.log', filemode='a', format='%(asctime)-15s %(message)s', level=logging.DEBUG)

class State(object):

	def __init__(self):
		self.unique_columns = ["DISTRICT", "HOSPITAL_NAME"]
		self.old_info_columns = ["LOCATION", "LAT", "LONG"]

	# def __new__(cls, state_name):
	# 	if(state_name == "TamilNadu"):
	# 		return super().__new__(TamilNadu)
	# 	else:
	# 		raise Exception("State {} not supported".format(state_name))

	def get_location_from_master(self, data):
		url = self.stein_url + "/" + self.main_sheet_name

		response = requests.get(url).json()
		data_with_loc_df = pd.DataFrame(response)
		data_df = pd.DataFrame(data)

		for unique_column in self.unique_columns:
			data_with_loc_df[unique_column] = data_with_loc_df[unique_column].str.strip()
			data_df[unique_column] = data_df[unique_column].str.strip()

		data_with_loc_df_subset = data_with_loc_df[self.unique_columns + self.old_info_columns + ["UID", "IS_NEW_HOSPITAL"]]
		merged_loc_df = pd.merge(data_df, data_with_loc_df_subset, 
									on=self.unique_columns, how="left")
		merged_loc_df["IS_NEW_HOSPITAL"] = merged_loc_df.apply(lambda row: True
												 if isinstance(row["IS_NEW_HOSPITAL"], float) else row["IS_NEW_HOSPITAL"], axis=1)

		merged_loc_df["UID"] = merged_loc_df.apply(lambda row: str(uuid.uuid4()) if row["IS_NEW_HOSPITAL"] else row["UID"], axis=1)

		merged_loc_df = self.tag_critical_care(merged_loc_df)
		merged_loc_df = merged_loc_df.fillna('')
		return merged_loc_df.to_dict('records')

	def push_data(self):

		logging.info("Pushing for state - "+ self.state_name)

		url = self.stein_url + "/" + self.custom_sheet_name
		n = 50

		logging.info("Fetching data from source")
		data = self.get_data_from_source()
		
		# data = self.get_dummy_data()
		logging.info("Fetching location from master sheet")
		location_tagged_data = self.get_location_from_master(data)

		nested_data = [location_tagged_data[i * n:(i + 1) * n] for i in range((len(location_tagged_data) + n - 1) // n )]

		logging.info("Posting data to Google Sheets")
		for each_data_point in nested_data:
			logging.info("Pushing 50 data points")
			x = requests.post(url, json = each_data_point)
			logging.info(x.text)


	def get_non_empty_dict_value(self, key, dict_obj):
		if key in dict_obj:
			return dict_obj[key]
		else:
			return ""
