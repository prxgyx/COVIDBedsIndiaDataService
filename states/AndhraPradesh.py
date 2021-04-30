import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import json
from .State import State

class AndhraPradesh(State):
    source_url = "http://dashboard.covid19.ap.gov.in/ims/hospbed_reports/process.php"
    stein_url = "https://stein.hamaar.cloud/v1/storages/608982e003eef31f34d05a71"
    main_sheet_name = "Andhra Pradesh"
    state_name = "AndhraPradesh"
    sheet_url = stein_url + "/" + main_sheet_name
    district_params = [
        {
            "city": "Anantapur",
            "hospdata": 1,
            "district": 502,
        },
        {
            "city": "Chittoor",
            "hospdata": 1,
            "district": 503,
        },
        {
            "city": "East godavari",
            "hospdata": 1,
            "district": 505,
        },
        {
            "city": "Guntur",
            "hospdata": 1,
            "district": 506,
        },
        {
            "city": "Krishna",
            "hospdata": 1,
            "district": 510,
        },
        {
            "city": "Kurnool",
            "hospdata": 1,
            "district": 511,
        },
        {
            "city": "Prakasam",
            "hospdata": 1,
            "district": 517,
        },
        {
            "city": "Spsr nellore",
            "hospdata": 1,
            "district": 515,
        },
        {
            "city": "Srikakulam",
            "hospdata": 1,
            "district": 519,
        },
        {
            "city": "Visakhapatanam",
            "hospdata": 1,
            "district": 520,
        },
        {
            "city": "Vizianagaram",
            "hospdata": 1,
            "district": 521,
        },
        {
            "city": "West godavari",
            "hospdata": 1,
            "district": 523,
        },
        {
            "city": "Y.S.R.",
            "hospdata": 1,
            "district": 504,
        },
    ]

    def __init__(self, *args, **kwargs):
        super().__init__()
        logging.info("Fetching data from Google Sheets")
        self.sheet_response = requests.get(self.sheet_url).json()
        self.number_of_records = len(self.sheet_response)
        logging.info("Fetched {} records from Google Sheets".format(self.number_of_records))

    def get_data_from_source(self):
        output_rows = []
        s_no = 0

        # Run for each district
        for district in self.district_params:
            response_html = requests.post(self.source_url, data=district)
            soup = BeautifulSoup(response_html.text, "html.parser")
            row_data = {}

            default_zero = lambda td: td.text or '0'

            # Add rows for the district
            for tr in soup.find_all('tr')[2:]:
                tds = tr.find_all('td')
                row_data = {
                    "SNO": s_no+1 ,
                    "DISTRICT": district['city'],
                    "HOSPITAL_NAME": tds[1].text,
                    "CONTACT": tds[2].text,
                    "AAROGYASRI_EMPANELMENT_STATUS": tds[3].text,
                    "ICU_TOTAL": default_zero(tds[4]),
                    "ICU_OCCUPIED": default_zero(tds[5]),
                    "ICU_AVAILABLE": default_zero(tds[6]),
                    "OXYGEN_GENERAL_TOTAL": default_zero(tds[7]),
                    "OXYGEN_GENERAL_OCCUPIED": default_zero(tds[8]),
                    "OXYGEN_GENERAL_AVAILABLE": default_zero(tds[9]),
                    "GENERAL_TOTAL": default_zero(tds[10]),
                    "GENERAL_OCCUPIED": default_zero(tds[11]),
                    "GENERAL_AVAILABLE": default_zero(tds[12]),
                    "VENTILATOR": default_zero(tds[13]),
                }
                s_no = s_no + 1
                output_rows.append(row_data)

        return pd.DataFrame(output_rows)

    def tag_critical_care(self, merged_loc_df):
        logging.info("Tagged critical care")
        merged_loc_df["HAS_ICU_BEDS"] = merged_loc_df.apply(lambda row: logging.info(row) or int(row["ICU_TOTAL"]) > 0, axis=1)
        merged_loc_df["HAS_VENTILATORS"] = merged_loc_df.apply(lambda row: int(row["VENTILATOR"]) > 0, axis=1)
        return merged_loc_df
