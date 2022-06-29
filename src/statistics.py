import pandas as pd

import filing
import company

def build_dataframe():
    #definiere Spalten
    CLMN_LEI = "LEI"
    CLMN_COMPANY = "COMPANY"
    CLMN_TRBC_SECTOR = "TRBC_SECTOR"
    CLMN_COUNTRY = "COUNTRY"
    CLMN_STOCK_EXCHANGE = "STOCK_EXCHANGE"
    CLMN_INDEX = "INDEX"
    CLMN_YEAR = "YEAR"
    CLMN_ALL_FACTS = "ALL_FACTS"
    CLMN_ESEF_FACTS = "ESEF_FACTS"
    CLMN_EXT_FACTS = "EXT_FACTS"

    data = {
        CLMN_LEI : [],
        CLMN_COMPANY : [],
        CLMN_TRBC_SECTOR : [],
        CLMN_COUNTRY : [],
        CLMN_STOCK_EXCHANGE : [],
        CLMN_INDEX : [],
        CLMN_YEAR : [],
        CLMN_ALL_FACTS : [],
        CLMN_ESEF_FACTS : [],
        CLMN_EXT_FACTS : [],
    }

    for l, c in company.COMPANIES.items():
        assert isinstance(c, company.Company)

        for year, f in c.esef_filings.items():
            assert isinstance(f, filing.ESEFFiling)

            data[CLMN_LEI].append(c.lei)
            data[CLMN_COMPANY].append(c.common_name)
            data[CLMN_TRBC_SECTOR].append(c.trbc_economic_sector)
            data[CLMN_COUNTRY].append(c.exchange_country)
            data[CLMN_STOCK_EXCHANGE].append(c.exchange_name)
            data[CLMN_INDEX].append("")
            data[CLMN_YEAR].append(year)
            data[CLMN_ALL_FACTS].append(len(f.facts))

            esef_facts = 0
            ext_facts = 0

            for fct in f.facts:
                assert isinstance(fct, filing.ESEFFact)

                if fct.is_extension:
                    ext_facts += 1
                else:
                    esef_facts += 1

            data[CLMN_ESEF_FACTS].append(esef_facts)
            data[CLMN_EXT_FACTS].append(ext_facts)

    return pd.DataFrame(data=data)

