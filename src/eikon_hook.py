import time

import pandas as pd

import eikon as ek

import company

EIKON_APP_KEY = "5470a751badf47c4bc9e3e92cd0f04843c1f6838"

def set_app_key():
    try:
        ek.set_app_key(EIKON_APP_KEY)
    except ek.EikonError as err:
        print("Es gibt ein Problem mit Refinitiv Eikon (Error code: {}): {}".format(err.code, err.message))

def get_company_data(comp):
        assert(isinstance(comp, company.Company))

        trf_common_name = ek.TR_Field("TR.CommonName")
        trf_business_summary = ek.TR_Field("TR.BusinessSummary")
        trf_naics_sector = ek.TR_Field("TR.NAICSSector")
        trf_exchange_country = ek.TR_Field("TR.ExchangeCountry")
        trf_exchange_name = ek.TR_Field("TR.ExchangeName")
        trf_company_market_cap = ek.TR_Field("TR.CompanyMarketCap",
        params = {
            "SDate" : "{}".format(comp.esef_filing.period_end),
            "Scale" : 6,
            "Curn" : "EUR"
        })
        trf_free_float_pct = ek.TR_Field("TR.FreeFloatPct")
        
        while True:
            try:
                data, err = ek.get_data("{}@LEI".format(comp.lei),
                    [
                        trf_common_name,
                        trf_business_summary,
                        trf_naics_sector,
                        trf_exchange_country,
                        trf_exchange_name,
                        trf_company_market_cap,
                        trf_free_float_pct
                    ])

                time.sleep(0.25) #Ausführung für 1/4 Sekunden anhalten, damit das Zugriffslimit der Eikon Data API nicht erreicht wird (höchsten 5 Anfragen je Sekunde und 10.000 am Tag)
                # Gesammelte Abfrage meiner Meinung nach nicht möglich, da Parameter Market Cap vom individuellen Stichtag des Unternehmens abhängig ist.

                return data.iloc[0]
            except ek.EikonError as err:
                if err.code != 400:
                    print("Es gibt ein Problem beim Datenabruf über Refinitiv Eikon (Error code: {}): {}".format(err.code, err.message))

                    return pd.Series(["n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"])