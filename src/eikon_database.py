import logging
import time
from typing import Dict, List, Tuple

import yaml

import pandas as pd

import eikon as ek

PATH_CONFIG_FILE = "./config.yml"

INDEX_ESEF_PACKAGE_NAME = 0
INDEX_LEI = 1
INDEX_PERIOD_END = 2
INDEX_ISIN = 10

def setup() -> bool:
    eikon_app_key = ""

    with open(PATH_CONFIG_FILE, "r") as yml_file:
        config = yaml.load(yml_file, Loader=yaml.Loader)

        eikon_app_key = config["eikon_app_key"]

    # Test umzu überprüfen, ob Refinitiv Eikon gestartet ist und die Verbindung hergestellt ist.
    try:
        ek.set_app_key(eikon_app_key)

        ek.get_data("529900NNUPAGGOMPXZ31@LEI", "TR.PriceClose")

        return True
    except:
            return False

def get_company_data(reports: list):
        for report in reports:
            
            # Definiere Datenfelder
            trf_isin = ek.TR_Field("TR.ISIN")
            trf_common_name = ek.TR_Field("TR.CommonName")
            trf_naics_sector = ek.TR_Field("TR.NAICSSector")
            trf_country = ek.TR_Field("TR.ExchangeCountry")
            trf_market_cap = ek.TR_Field("TR.CompanyMarketCap",
                params = {
                    "Scale" : 6,
                    "Curn" : "EUR"
                })
            trf_free_float = ek.TR_Field("TR.FreeFloatPct")
            trf_auditor = ek.TR_Field("TR.F.Auditor")
            trf_auditor_fees = ek.TR_Field("TR.F.AuditorFees",
                params = {
                    "Curn" : "EUR"
                })
            trf_employees = ek.TR_Field("TR.CompanyNumEmploy")
            trf_founded = ek.TR_Field("TR.OrgFoundedYear")
            trf_analysts_following = ek.TR_Field("TR.NumberOfAnalysts")
            trf_total_assets = ek.TR_Field("TR.F.TotAssets",
                params = {
                    "Scale" : "6",
                    "Curn" : "EUR"
                })
            trf_total_debt = ek.TR_Field("TR.F.DebtTot",
                params = {
                    "Scale" : "6",
                    "Curn" : "EUR"
                })
            trf_income = ek.TR_Field("TR.F.IncBefDiscOpsExordItems",
                params = {
                    "Scale" : "6",
                    "Curn" : "EUR"
                })
            trf_total_assets_t_1 = ek.TR_Field("TR.F.TotAssets",
                params = {
                    "Scale" : "6",
                    "Curn" : "EUR"
                })

            instrument_lei = report[INDEX_LEI] + "@LEI"
            instrument_isin = ""

            # Abschlussstichtag der Berichtsperiode
            period_end = str(report[INDEX_PERIOD_END])

            # Abschlussstichtag des Vorjahres berechnen
            year = period_end[:4]
            year = int(year)
            year -= 1
            period_end_t_1 = str(year) + period_end[4:]
            
            # Zäht die Anzahl der versuchten Datenabrufe und definiert die maximale Anzahl an Versuchen
            tries = 1
            max_tries = 5

            print("\nDie zum ESEF-Paket \"{}\" zugehörigen Unternehmensdaten werden nun aus Refinitiv Eikon heruntergeladen.".format(report[INDEX_ESEF_PACKAGE_NAME]))

            step1 = False
            step2 = False

            while True:
                try:
                    if not step1:
                        _get_tr_fields(report, instrument_lei, [trf_isin], {})

                        step1 = True

                        try:
                            instrument_isin = report[INDEX_ISIN]

                            if pd.isnull(instrument_isin):
                                instrument_isin = ""
                        except IndexError:
                            pass
                        
                        
                        # Gesammelte Abfrage für alle Unternehmen meiner Meinung nach nicht möglich, da Parameter Market Cap vom individuellen Stichtag des Unternehmens abhängig ist (je Abfrage kann nur ein Zeitpunkt angeben werden).

                    if not step2:
                        # Zweiter Datenabruf notwendig, da das Datenfeld "TR.F.Auditor" und weitere Felder über den Identifier LEI nicht verfügbar sind. Hierfür muss im ersten Schritt die ISIN ermittelt werden.

                        _get_tr_fields(report, instrument_isin,
                            [
                                trf_common_name,
                                trf_naics_sector,
                                trf_country,
                                trf_market_cap,
                                trf_free_float,
                                trf_auditor,
                                trf_auditor_fees,
                                trf_employees,
                                trf_founded,
                                trf_analysts_following,
                                trf_total_assets,
                                trf_total_debt,
                                trf_income,
                            ], {"SDate" : period_end})

                        step2 = True
                        
                    _get_tr_fields(report, instrument_isin, [trf_total_assets_t_1], {"SDate" : period_end_t_1})

                    print("\n\t==> Unternehmendaten erfolgreich heruntergeladen.")
                    break          

                except ek.EikonError as err:
                    print("\n\t==> Es ist ein Serverfehler (Error {}) beim Datenabruf von Refinitiv Eikon aufgetreten: {}".format(err.code, err.message))

                if tries < max_tries:
                    tries += 1 
                    print("\n\tDatenabruf wird erneut versucht... (Versuch {}/{})".format(tries, max_tries))
                else:
                    print("\n\t==> Datenabruf für die zum ESEF-Paket \"{}\" gehörigen Unternehmensdaten nicht möglich. Unternehmen wird übersprungen.".format(report[INDEX_ESEF_PACKAGE_NAME]))

                    break
                    
def _get_tr_fields(report: List, instrument: str, tr_fields: List, params: Dict):
    
    if not instrument:
        print("\n\t==> Achtung, es konnten keine Daten zu dem Unternehmen geladen werden, da der Instrument-Identifier nicht ermittelbar ist.")

        for field in tr_fields:
            report.append("")

        return
    
    # Zugriffszeit messen, damit das Zugriffslimit der Eikon Data API nicht erreicht wird (höchsten 5 Anfragen je Sekunde und 10.000 am Tag)
    start = time.time()

    # Serverfehler werden über das Abfangen des ek.EikonErrors von der aufrufenden Funktion behandelt
    data, err = ek.get_data(instrument, tr_fields, parameters=params)

    end = time.time()

    duration = end - start

    if duration < 0.2:
        time.sleep(0.2 - duration)

    i = 0
    for index, value in data.iloc[0].iteritems():
        if i == 0 and index != "ISIN":
            continue
        
        print("Index: {}".format(index))
        print("Append: {}".format(value))

        report.append(value)
        i += 1

    if err:
        print("\n\t==> Refiniv Eikon hat einen oder mehrere Fehler als Antwort auf den Datenabruf gesendet:")

        for e in err:
            print("\n\t\t{} (Error {})".format(e["message"], e["code"]))
        
        print("\n\tBitte überprüfen Sie die Daten.")