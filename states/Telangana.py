import urllib3
from bs4 import BeautifulSoup
import requests
from states.State import State

class Telangana(State):

	def __init__(self):
		self.stein_url = "https://stein.hamaar.cloud/v1/storages/6089829403eef36d93d05a6f"
		self.source_url = "http://164.100.112.24/SpringMVC/getHospital_Beds_Status_Citizen.htm"
		self.main_sheet_name = "Sheet14"
