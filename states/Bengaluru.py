import time
import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

url='https://bbmpgov.com/chbms'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
soup.prettify()

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