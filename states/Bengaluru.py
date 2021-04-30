import time
import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import logging
import json
from states.State import State

class Bengaluru(State):

    def __init__(self):
        super().__init__()
        self.state_name = "Bengaluru"
        self.stein_url = "https://stein.hamaar.cloud/v1/storages/608982f703eef3de2bd05a72"
        self.source_url = "https://bbmpgov.com/chbms"
        self.main_sheet_name = "Bengaluru"
        self.unique_columns = ["HOSPITAL_NAME"]
        self.old_info_columns = ["LOCATION"]
        self.sheet_url = self.stein_url + "/" + self.main_sheet_name
        # Fetching it here because need number of records in the Class
        # need number of records because bulk delete API throws error entity too large
        logging.info("Fetching data from Google Sheets")
        self.sheet_response = requests.get(self.sheet_url).json()
        self.number_of_records = len(self.sheet_response)
        logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))

    def get_data_from_source(self):
        response = requests.get(self.source_url)
        soup = BeautifulSoup(response.text, 'html.parser')

        tables = soup.find_all("tbody")
        tables_head = soup.find_all("thead")
        types=['Government Hospitals (Covid Beds)','Government Medical Colleges (Covid Beds)',
               'Private Hospitals (Government Quota Covid Beds)','Private Medical Colleges (Government Quota Covid Beds)']

        finaldf=pd.DataFrame()
        for i in range(len(types)):
            storeTable = tables_head[i+2].find_all("tr")
            storeValueRows = tables[i+2].find_all("tr")

            storeMatrix = []
            for row in storeValueRows:
            #     print(row)
                storeMatrixRow = []
                for cell in row.find_all("td"):
                    storeMatrixRow.append(cell.get_text().strip())
                storeMatrix.append(storeMatrixRow)
            storeMatrix=pd.DataFrame(storeMatrix)
            storeMatrix['Type']=types[i]
            storeMatrix=storeMatrix.drop((len(storeMatrix)-1),axis=0)
            
            finaldf=pd.concat([finaldf,storeMatrix],axis=0)
        finaldf.columns=['sno','HOSPITAL_NAME','ALLOCATED_BEDS_GEN',
                         'ALLOCATED_BEDS_HDU','ALLOCATED_BEDS_ICU','ALLOCATED_BEDS_VENT',
                         'ALLOCATED_BEDS_TOTAL','OCCUPIED_BEDS_GEN','OCCUPIED_BEDS_HDU','OCCUPIED_BEDS_ICU',
                         'OCCUPIED_BEDS_VENT','OCCUPIED_BEDS_TOTAL','AVAILABLE_BEDS_GEN','AVAILABLE_BEDS_HDU','AVAILABLE_BEDS_ICU',
                         'AVAILABLE_BEDS_VENT','AVAILABLE_BEDS_TOTAL','TYPE']
        finaldf=finaldf.drop('sno',axis=1)

        finaldf.reset_index(drop=True)

        # output_json = json.loads(finaldf.to_json(orient="records"))

        return finaldf

    def tag_critical_care(self, merged_loc_df):
        logging.info("Tagged critical care")
        merged_loc_df["HAS_ICU_BEDS"] = merged_loc_df.apply(lambda row: int(row["ALLOCATED_BEDS_ICU"]) > 0, axis=1)
        merged_loc_df["HAS_VENTILATORS"] = merged_loc_df.apply(lambda row: int(row["ALLOCATED_BEDS_VENT"]) > 0, axis=1)
        return merged_loc_df