import eikon as ek

import pandas as pd

EIKON_APP_KEY = "5470a751badf47c4bc9e3e92cd0f04843c1f6838"
ek.set_app_key(EIKON_APP_KEY)

class Company:
    def __init__(self, lei):
        self.lei = lei
        self.common_name = None
        self.business_summary = None
        self.trbc_economic_sector = None
        self.exchange_country = None
        self.exchange_name = None
        self.company_market_cap = {}
        self.esef_filings = []

    def get_company_data(self):
        trf_common_name = ek.TR_Field("TR.CommonName")
        trf_business_summary = ek.TR_Field("TR.BusinessSummary")
        trf_trbc_economic_sector = ek.TR_Field("TR.TRBCEconomicSector")
        trf_exchange_country = ek.TR_Field("TR.ExchangeCountry")
        trf_exchange_name = ek.TR_Field("TR.ExchangeName")
        trf_company_market_cap_2020 = ek.TR_Field("TR.CompanyMarketCap",
        params = {
            "SDate" : "20201231",
            "Scale" : 6,
            "Curn" : "EUR"
        })
        trf_company_market_cap_2021 = ek.TR_Field("TR.CompanyMarketCap",
        params = {
            "SDate" : "20211231",
            "Scale" : 6,
            "Curn" : "EUR"
        })
        
        data, err = ek.get_data("{}@LEI".format(self.lei),
            [
                trf_common_name,
                trf_business_summary,
                trf_trbc_economic_sector,
                trf_exchange_country,
                trf_exchange_name,
                trf_company_market_cap_2020,
                trf_company_market_cap_2021
            ])

        fields = data.iloc[0]

        self.common_name = fields[1]
        self.business_summary = fields[2]
        self.trbc_economic_sector = fields[3]
        self.exchange_country = fields[4]
        self.exchange_name = fields[5]
        self.company_market_cap["2020"] = fields[6]
        self.company_market_cap["2021"] = fields[7]

class ESEF_Filing:
    def __init__(self, period):
        self.period = period
        self.elements = []
        self.extensions = []