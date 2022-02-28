import time
import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
from states.State import State
import logging

class Gandhinagar(State):

    def __init__(self, test_prefix=None):
        self.state_name = "Gandhinagar"
        super().__init__()
        self.source_url = "https://vmc.gov.in/HospitalModuleGMC/HospitalBedsDetails.aspx?tid=1"
        self.main_sheet_name = "Gandhinagar"
        if test_prefix:
            self.main_sheet_name = test_prefix + self.main_sheet_name
        self.unique_columns = ["HOSPITAL_NAME", "AREA"]
        self.sheet_url = self.stein_url + "/" + self.main_sheet_name
        # Fetching it here because need number of records in the Class
        # need number of records because bulk delete API throws error entity too large
        logging.info("Fetching data from Google Sheets")
        self.sheet_response = requests.get(self.sheet_url).json()
        self.number_of_records = len(self.sheet_response)
        logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))
        self.icu_beds_column = "ICU_WITHOUT_VENTILATOR_TOTAL"
        self.vent_beds_column = "ICU_WITH_VENTILATOR_AVAILABLE"

    def get_data_from_source(self):
        response = requests.get(self.source_url)
        soup = BeautifulSoup(response.text, 'html.parser')

        tables = soup.find_all("tr")
        storeMatrix=[]
        for row in tables:
            try:
                storeMatrixRow=[]
                for cell in row.find_all("td"):
                    storeMatrixRow.append(cell.get_text().strip())
                storeMatrixRow.append(row.find_all('a')[0]['href'])
                storeMatrix.append(storeMatrixRow)
            except:
                logging.info('skipped header row')
            
        storeMatrix=pd.DataFrame(storeMatrix)
        storeMatrix.columns=['HOSPITAL_NAME','AREA','TOTAL_BEDS','TOTAL_OCCUPIED','TOTAL_VACANT','NODAL_OFFICER_NAME',
                            'NODAL_OFFICER_NUMBER','LAST_UPDATED','HOSP_ID']

        beddata=[]
        for hospital in soup.find_all('a'):
            hosp=hospital['href']
            innerURL='https://vmc.gov.in/HospitalModuleGMC/'+hosp
            innerresponse = requests.get(innerURL)
            innersoup = BeautifulSoup(innerresponse.text, 'html.parser')
            tables = innersoup.find_all("table")
            storeValueRows = tables[1].find_all("tr")
            hospMatrix = []
            for row in storeValueRows:
                hospMatrixRow = []
                for cell in row.find_all("td"):
                    hospMatrixRow.append(cell.get_text().strip())
                hospMatrix.append(hospMatrixRow)
            hospMatrix=pd.DataFrame(hospMatrix)
            hospMatrix=hospMatrix.rename(columns=hospMatrix.iloc[0])
            newrow=[hospMatrix.iloc[1,2],hospMatrix.iloc[1,3],hospMatrix.iloc[1,4],hospMatrix.iloc[1,5],
                   hospMatrix.iloc[3,2],hospMatrix.iloc[3,3],hospMatrix.iloc[3,4],hospMatrix.iloc[3,5],hosp]
            beddata.append(newrow)

        beddata=pd.DataFrame(beddata)
        beddata.columns=['ICU_WITH_VENTILATOR_TOTAL','ICU_WITHOUT_VENTILATOR_TOTAL','O2_BEDS_TOTAL','MILD_SYMPTOMS_BED_TOTAL',
                        'ICU_WITH_VENTILATOR_AVAILABLE','ICU_WITHOUT_VENTILATOR_AVAILABLE','O2_BEDS_AVAILABLE',
                        'MILD_SYMPTOMS_BED_AVAILABLE','HOSP_ID']

        finaldf=storeMatrix.merge(beddata,on='HOSP_ID',how='left')

        return finaldf

