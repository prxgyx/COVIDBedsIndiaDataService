import urllib
import json
import requests

class State(object):

	# def __new__(cls, state_name):
	# 	if(state_name == "TamilNadu"):
	# 		return super().__new__(TamilNadu)
	# 	else:
	# 		raise Exception("State {} not supported".format(state_name))

	def get_location_from_master(self, hsp_info):
		url = self.stein_url + "/" + self.main_sheet_name

		hsp_name = hsp_info["HOSPITAL_NAME"].strip()
		hsp_district = hsp_info["DISTRICT"]

		query = {
			"DISTRICT": hsp_district,
			"HOSPITAL_NAME": hsp_name
		}

		encode_query = urllib.parse.quote_plus(json.dumps(query))

		query_url = url + "?search={}".format(encode_query)
		
		hsp_results = requests.get(query_url).json()

		if (len(hsp_results) > 1):
			print("More than 1 row fetched for {}, {}".format(hsp_name, hsp_district))

		if "error" in hsp_results:
			print(hsp_name, hsp_district)
			print(hsp_results)

		if hsp_results and "LOCATION" in hsp_results[0]:
			# print(hsp_results)
			hsp_result = hsp_results[0]
			loc = hsp_result["LOCATION"]
			lat = hsp_result["LAT"]
			lng = hsp_result["LONG"]
		else:
			if not hsp_results:
				print("No location fetched for {}, {}".format(hsp_name, hsp_district))
			else:
				print("location is empty for {}, {}".format(hsp_name, hsp_district))
			loc = ""
			lat = ""
			lng = ""

		hsp_info["LOCATION"] = loc
		hsp_info["LAT"] = lat
		hsp_info["LONG"] = lng

		self.tag_critical_care(hsp_info)

		return hsp_info

	def push_data(self):

		url = self.stein_url + "/" + self.custom_sheet_name
		n = 50

		print("Fetching data from source")
		data = self.get_data_from_source()
		# data = self.get_dummy_data()

		location_tagged_data = [self.get_location_from_master(i) for i in data]

		print("Fetching location from master sheet")
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
