import requests

data_source = "https://beds.dgmhup-covid19.in/EN/covid19bedtrack"
api = "https://api.covidbedsindia.in/v1/storages/609fc7dde75f9ccdd696eb35/Uttar%20Pradesh"

desc_api = "https://stein.hamaar.cloud/v1/storages/608982f703eef3de2bd05a72/Temp"

md_beg = "## Data Source\n{}\n\n ## Response fields \n\n |Field|Description|Example|\n|---|---|---|\n".format(data_source)

main_api_resp = requests.request("GET", api).json()[0]

desc_api_resp = requests.request("GET", desc_api).json()

desc_api_dict = {}

for resp in desc_api_resp:
	field = resp['field']
	desc = resp['desc']
	desc_api_dict[field] = desc

# print(desc_api_dict)

for field,example in main_api_resp.items():
	field_desc = desc_api_dict.get(field)
	if not field_desc:
		field_desc = ""
	md_row = "|{}|{}|{}|\n".format(xfield, field_desc, example)
	md_beg = md_beg + md_row

print(md_beg)