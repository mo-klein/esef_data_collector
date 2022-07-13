import pandas as pd

import eikon as ek

EIKON_APP_KEY = "5470a751badf47c4bc9e3e92cd0f04843c1f6838"

def set_app_key():
    try:
        ek.set_app_key(EIKON_APP_KEY)
    except ek.EikonError as err:
        print("Es gibt ein Problem mit Refinitiv Eikon (Error code: {}): {}".format(err.code, err.message))

def get_company_data(lei):
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
        
        try:
            data, err = ek.get_data("{}@LEI".format(lei),
                [
                    trf_common_name,
                    trf_business_summary,
                    trf_trbc_economic_sector,
                    trf_exchange_country,
                    trf_exchange_name,
                    trf_company_market_cap_2020,
                    trf_company_market_cap_2021
                ])

            return data.iloc[0]
        except ek.EikonError as err:
            print("Es gibt ein Problem beim Datenabruf über Refinitiv Eikon (Error code: {}): {}".format(err.code, err.message))

            return pd.Series(["n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"])