import json
import os

import numpy as np

import eikon_hook

PATH_COMPANIES = "./companies"

class Company():
    def __init__(self, lei):
        self.lei = lei
        self.common_name = None
        self.business_summary = None
        self.trbc_economic_sector = None
        self.exchange_country = None
        self.exchange_name = None
        self.company_market_cap = {}
        self.esef_filings = {}

    def get_company_data(self):

        fields = eikon_hook.get_company_data(self.lei)

        self.common_name = fields[1]
        self.business_summary = fields[2]
        self.trbc_economic_sector = fields[3]
        self.exchange_country = fields[4]
        self.exchange_name = fields[5]
        self.company_market_cap["2020"] = fields[6]
        self.company_market_cap["2021"] = fields[7]

def create_companies(filings):
    companies = {}

    for f in filings:
        c = companies.get(f.lei)
        new_c = False

        if c is None:
            c = Company(f.lei)
            new_c = True
        
        c.esef_filings[f.period_end] = f

        c.get_company_data()

        if new_c:
            companies[f.lei] = c

    return companies

def _serialize(obj):
    if isinstance(obj, np.int64): #in manchen Fällen hat der Marktwert des Unternehmens den Typ numpy.int64
        return obj.item() #numpy.int64 in int konvertieren

    return vars(obj)

def save_companies(companies):
    try:
        os.mkdir(PATH_COMPANIES)
    except FileExistsError:
        pass

    for k, v in companies.items():

        print("Saving {} to json.".format(v.common_name))

        with open("{}/{}.json".format(PATH_COMPANIES, k), "w") as file:
            json.dump(v, file, default=_serialize, indent=4)