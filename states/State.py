import urllib
import json
import requests
import pandas as pd
import logging
import uuid
import sys
import os
import datetime
from states.notification.TelegramBot import TelegramBot
import os


logging.basicConfig(filename='DataService.log', filemode='a', format='%(asctime)-15s %(message)s', level=logging.DEBUG)
covidbedsbot = TelegramBot()

class State(object):

	def __init__(self):
		self.unique_columns = ["DISTRICT", "HOSPITAL_NAME"]
		self.old_info_columns = ["LOCATION", "LAT", "LONG"]
		# To delete all records
		self.delete_condition = {"condition": {}, "limit": 50}
		self.is_fresh = False
		self.stein_url_id = os.environ[self.state_name]
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/{}".format(self.stein_url_id)

	def get_uid_lastsynced(self, merged_loc_df):
		merged_loc_df["UID"] = merged_loc_df.apply(lambda row: row["UID"] if (isinstance(row["UID"], str) and 
													row["UID"]!="") else str(uuid.uuid4()), axis=1)
		merged_loc_df["LAST_SYNCED"] = pd.to_datetime('now').replace(microsecond=0) + pd.Timedelta('05:30:00')
		merged_loc_df["LAST_SYNCED"] = merged_loc_df["LAST_SYNCED"].astype(str)
		return merged_loc_df


	def get_location_from_master(self, govt_data_df, sheet_data_df):
		unique_columns_lower = []
		for unique_column in self.unique_columns:
			sheet_data_df[unique_column] = sheet_data_df[unique_column].str.strip()
			govt_data_df[unique_column] = govt_data_df[unique_column].str.strip()
			sheet_data_df[unique_column+"_lower"] = sheet_data_df[unique_column].str.lower()
			govt_data_df[unique_column+"_lower"] = govt_data_df[unique_column].str.lower()
			unique_columns_lower.append(unique_column+"_lower")


		sheet_data_df_subset = sheet_data_df[unique_columns_lower+ self.old_info_columns + ["UID", "IS_NEW_HOSPITAL"]]

		bool_values = {"TRUE":True, "FALSE": False}
		sheet_data_df_subset["IS_NEW_HOSPITAL"] = sheet_data_df_subset["IS_NEW_HOSPITAL"].map(bool_values)

		merged_loc_df = pd.merge(govt_data_df, sheet_data_df_subset, 
									on=unique_columns_lower, how="left")
		merged_loc_df["IS_NEW_HOSPITAL"] = merged_loc_df["IS_NEW_HOSPITAL"].fillna(value=True)

		merged_loc_df = self.tag_critical_care(merged_loc_df)
		merged_loc_df = merged_loc_df.fillna('')
		merged_loc_df = merged_loc_df.drop(unique_columns_lower, axis=1)
		merged_loc_df["STEIN_ID"] = self.state_name
		merged_loc_df = merged_loc_df.drop_duplicates(self.unique_columns)

		merged_loc_df = self.get_uid_lastsynced(merged_loc_df)	
		return merged_loc_df.to_dict('records')


	def push_data(self):

		logging.info("Pushing for state - "+ self.state_name)

		logging.info("Fetching data from source")
		govt_data_df = self.get_data_from_source()

		self.inner_push_data(govt_data_df)

	def inner_push_data(self, govt_data):
		sheet_data_df = pd.DataFrame(self.sheet_response)
		if len(govt_data) > 0:
			self.get_error_message(self.sheet_response)

			if self.is_fresh:
				location_uid_synced_data = self.add_uid_lastsynced(govt_data)
			else:
				logging.info("Fetching location from master sheet")
				location_uid_synced_data = self.get_location_from_master(govt_data, sheet_data_df)
				
			new_df_len = len(location_uid_synced_data)

			if len(sheet_data_df)*.9 > new_df_len:

				failure_reason = "Row count with the scraped data is low ({}), can cause data loss, Omitting writing to main file".format(new_df_len)
				logging.info(failure_reason)
				self.error_msg_info(failure_reason, sheet_data_df)
				covidbedsbot.send_message(self.error_msg_info(failure_reason, sheet_data_df))
			else:
				self.write_temp_file(sheet_data_df)

				delete_data_response = self.delete_data_from_sheets()

				# Successful delete
				if "clearedRowsCount" in delete_data_response:
					self.push_data_to_sheets(location_uid_synced_data, 50)
					covidbedsbot.send_message(self.success_msg_info(sheet_data_df, location_uid_synced_data))
					covidbedsbot.send_local_file("tmp_{}".format(self.state_name))
				else:
					failure_reason = "Error deleting data from sheets - {}".format(delete_data_response)
					covidbedsbot.send_message(self.error_msg_info(failure_reason, sheet_data_df))

		else:
			failure_reason = "No data retrieved from url"
			logging.info(failure_reason)
			covidbedsbot.send_message(self.error_msg_info(failure_reason, sheet_data_df))


	def success_msg_info(self, sheet_data_df, location_tagged_data):
		success_msg = u'\u2714'+ " Run successful for "+ self.state_name +"\n"
		success_msg += u'\u2022' + " Previous record count - "+str(len(sheet_data_df)) + "\n"
		success_msg += u'\u2022' + " Current record count - "+str(len(location_tagged_data)) +"\n"

		IS_NEW_HOSPITAL_COUNT = len([x for x in location_tagged_data if x["IS_NEW_HOSPITAL"]])
		success_msg += u'\u2022' + " Hospitals with IS_NEW_HOSPITAL true - "+str(IS_NEW_HOSPITAL_COUNT) +"\n"
		success_msg += u'\u2022' + " Synced at - "+ location_tagged_data[0]["LAST_SYNCED"]
		return success_msg

		
	def error_msg_info(self, failure_reason, sheet_data_df):
		error_msg = u'\u274c'+ " Run failed for "+self.state_name+"\n"
		error_msg += u'\u2022' + " ERROR: "+ failure_reason+"\n"
		error_msg += u'\u2022' + " No data is updated to the sheet"+"\n"
		error_msg += u'\u2022' + " Previous record count - "+str(len(sheet_data_df)) + "\n"
		error_msg += u'\u2022' + " The sheet was last updated at "+sheet_data_df.iloc[0]["LAST_SYNCED"]
		return error_msg


	
	def tag_critical_care(self, merged_loc_df):
		logging.info("Tagged critical care")
		merged_loc_df[self.vent_beds_column] = merged_loc_df[self.vent_beds_column].fillna(value=0)
		merged_loc_df[self.icu_beds_column] = merged_loc_df[self.icu_beds_column].fillna(value=0)
		merged_loc_df["HAS_ICU_BEDS"] = merged_loc_df.apply(lambda row: int(row[self.icu_beds_column]) + int(row[self.vent_beds_column]) > 0, axis=1)
		merged_loc_df["HAS_VENTILATORS"] = merged_loc_df.apply(lambda row: int(row[self.vent_beds_column]) > 0, axis=1)
		return merged_loc_df


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
		self.delete_condition["condition"] = {"STEIN_ID": self.state_name}

		for i in range(0, number_of_loops):
			response = requests.delete(self.sheet_url, data=json.dumps(self.delete_condition)).json()
			logging.info(response)
			if "clearedRowsCount" not in response:
				return response
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
