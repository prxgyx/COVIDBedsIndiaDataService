import urllib
import json
import requests
import pandas as pd
import logging

class State(object):

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
		print(data_df)

		data_with_loc_df.HOSPITAL_NAME = data_with_loc_df.HOSPITAL_NAME.str.strip()
		data_with_loc_df.DISTRICT = data_with_loc_df.DISTRICT.str.strip()
		data_df.HOSPITAL_NAME = data_df.HOSPITAL_NAME.str.strip()
		data_df.DISTRICT = data_df.DISTRICT.str.strip()
		data_with_loc_df_subset = data_with_loc_df[["HOSPITAL_NAME", "DISTRICT", "LOCATION", "LAT", "LONG"]]
		data_with_loc_df_subset["IS_NEW_HOSPITAL"] = False
		merged_loc_df = pd.merge(data_df, data_with_loc_df_subset, 
									on=["HOSPITAL_NAME", "DISTRICT"], how="left")
		merged_loc_df["IS_NEW_HOSPITAL"] = merged_loc_df.apply(lambda row: True
												 if isinstance(row["IS_NEW_HOSPITAL"], float) else row["IS_NEW_HOSPITAL"], axis=1)

		merged_loc_df = self.tag_critical_care(merged_loc_df)
		merged_loc_df = merged_loc_df.fillna('')
		return merged_loc_df.to_dict('records')

	def push_data(self):

		url = self.stein_url + "/" + self.custom_sheet_name
		n = 50

		print("Fetching data from source")
		data = self.get_data_from_source()
		
		# data = self.get_dummy_data()
		print("Fetching location from master sheet")
		location_tagged_data = self.get_location_from_master(data)

		nested_data = [location_tagged_data[i * n:(i + 1) * n] for i in range((len(location_tagged_data) + n - 1) // n )]

		print("Posting data to Google Sheets")
		for each_data_point in nested_data:
			print("Pushing 50 data points")
			x = requests.post(url, json = each_data_point)
			print(x.text)


	def get_non_empty_dict_value(self, key, dict_obj):
		if key in dict_obj:
			return dict_obj[key]
		else:
			return ""
