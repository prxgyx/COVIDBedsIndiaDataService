import time
import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

url='https://vmc.gov.in/HospitalModuleGMC/HospitalBedsDetails.aspx?tid=1'
response = requests.get(url)
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
        print('skipped header row')
    
storeMatrix=pd.DataFrame(storeMatrix)
# storeMatrix=storeMatrix.drop(0,axis=0)
# storeMatrix['hospID']=hosp
storeMatrix.columns=['HOSPITAL_NAME','AREA','TOTAL_BEDS','TOTAL_OCCUPIED','TOTAL_VACANT','NODAL_OFFICER_NAME',
                    'NODAL_OFFICER_NUMBER','LAST_UPDATED','HOSP_ID']
# storeMatrix

beddata=[]
for hospital in soup.find_all('a'):
#     print(hospital)
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
                'ICU_WITH_VENTILATOR_AVAILABLE','ICU_WITHOUT_VENTILATOR_AVAILABLE','O2_BEDS_AVAILABLE'
                 ,'MILD_SYMPTOMS_BED_AVAILABLE','HOSP_ID']
# beddata.head()

finaldf=storeMatrix.merge(beddata,on='HOSP_ID',how='left')
finaldf.head()

