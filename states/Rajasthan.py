import time
import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

url='https://covidinfo.rajasthan.gov.in/Covid-19hospital-wisebedposition-wholeRajasthan.aspx'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

tables = soup.find_all("tbody")
storeValueRows = tables[0].find_all("tr")

storeMatrix = []
for row in storeValueRows:
#     print(row)
    storeMatrixRow = []
    for cell in row.find_all("td"):
        storeMatrixRow.append(cell.get_text().strip())
    storeMatrix.append(storeMatrixRow)
storeMatrix=pd.DataFrame(storeMatrix)

storeMatrix.columns=['sno','DISTRICT','HOSPITAL_NAME','GENERAL_BEDS_TOTAL','GENERAL_BEDS_OCCUPIED','GENERAL_BEDS_AVAILABLE',
                     'OXYGEN_BEDS_TOTAL','OXYGEN_BEDS_OCCUPIED','OXYGEN_BEDS_AVAILABLE','ICU_BEDS_TOTAL','ICU_BEDS_OCCUPIED',
                     'ICU_BEDS_AVAILABLE','VENTILATOR_BEDS_TOTAL','VENTILATOR_BEDS_OCCUPIED','VENTILATOR_BEDS_AVAILABLE',
                     'HOSPITAL_HELPLINE_NO','DISTRICT_CONTROL_ROOM']
storeMatrix=storeMatrix.drop(0,axis=0)
storeMatrix=storeMatrix.drop('sno',axis=1)
storeMatrix.head()