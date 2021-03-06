from states.State import State
import logging
import pandas as pd
import requests
from states.notification.TelegramBot import TelegramBot
import datetime
import uuid

covidbedsbot = TelegramBot()

class FreshState(State):

	def __init__(self, test_prefix=None):
		super().__init__()
		self.is_fresh = True

	def add_uid_lastsynced(self, data):
		now  = datetime.datetime.now()
		for hosp_info in data:
			hosp_info["UID"] = str(uuid.uuid4())
			hosp_info["LAST_SYNCED"] = now.strftime("%Y-%m-%d, %H:%M:%S")
			hosp_info["IS_NEW_HOSPITAL"] = False
		return data

	def get_master_sheet_df(self):
		logging.info("Fetching data from Google Sheets")
		master_sheet_url = self.stein_url + "/Master" 
		master_sheet_response = requests.get(master_sheet_url).json()
		return pd.DataFrame(master_sheet_response)

	def join_master_df(self, df):
		logging.info("Joining master sheet DF")
		master_df = self.get_master_sheet_df()
		unique_columns_lower = self.unique_columns

		merged_df = pd.merge(df, master_df, on=unique_columns_lower, how="left")
		merged_df['LAT'] = merged_df['LAT_y'].fillna(merged_df['LAT_x'])
		merged_df['LONG'] = merged_df['LONG_y'].fillna(merged_df['LONG_x'])
		merged_df = merged_df.drop(['LAT_x', 'LAT_y', 'LONG_x', 'LONG_y'], axis=1)

		return merged_df

	def success_msg_info(self, sheet_data_df, location_tagged_data):
		msg_info = super().success_msg_info(sheet_data_df, location_tagged_data)
		invalid_lat_long_count = len([x for x in location_tagged_data if x["LAT"] == "0"])
		msg_info += "\n" + u'\u2022' + " Count of facilities with invalid location - "+ str(invalid_lat_long_count)
		return msg_info
