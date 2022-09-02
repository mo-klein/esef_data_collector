import os

import matplotlib.pyplot as plt

import pandas as pd

import numpy as np

import filing

PATH_OUTPUT_DIR = "./output/"
PATH_SF = PATH_OUTPUT_DIR + "sample.xlsx"

PATH_COUNTRIES_DIR = PATH_OUTPUT_DIR + "/countries/"
PATH_SECTORS_DIR = PATH_OUTPUT_DIR + "/sectors/"

#definiere Spalten des "Haupt-DataFrames"
CLMN_LEI = "LEI"
CLMN_COMPANY = "COMPANY"
CLMN_NAICS_SECTOR = "NAICS_SECTOR"
CLMN_COUNTRY = "COUNTRY"
CLMN_STOCK_EXCHANGE = "STOCK_EXCHANGE"
CLMN_PERIOD_END = "PERIOD_END"
CLMN_MARKET_CAP = "MARKET_CAP"
CLMN_FREE_FLOAT = "FREE_FLOAT"
CLMN_ALL_FACTS = "ALL_FACTS"
CLMN_ALL_FACTS_PCT = "ALL_FACTS_PCT"
CLMN_ESEF_FACTS = "ESEF_FACTS"
CLMN_ESEF_FACTS_PCT = "ESEF_FACTS_PCT"
CLMN_EXT_FACTS = "EXT_FACTS"
CLMN_EXT_FACTS_PCT = "EXT_FACTS_PCT"
CLMN_SHA1 = "SHA1"

def load_dataframe():
    if os.path.exists(PATH_SF):
        return pd.read_excel(PATH_SF, index_col=0)
    else:

        data = {
            CLMN_LEI : [],
            CLMN_COMPANY : [],
            CLMN_NAICS_SECTOR : [],
            CLMN_COUNTRY : [],
            CLMN_STOCK_EXCHANGE : [],
            CLMN_PERIOD_END : [],
            CLMN_MARKET_CAP: [],
            CLMN_FREE_FLOAT: [],
            CLMN_ALL_FACTS : [],
            CLMN_ALL_FACTS_PCT: [],
            CLMN_ESEF_FACTS : [],
            CLMN_ESEF_FACTS_PCT : [],
            CLMN_EXT_FACTS : [],
            CLMN_EXT_FACTS_PCT : [],
            CLMN_SHA1: [],
        }

        return pd.DataFrame(data=data)

"""Baut das "Haupt-DataFrame" mit allen Unternehmen. Dieses DF wird dann im Programmablauf entsprechend des aktuellen Untersuchungsgegenstandes modifiziert."""

def append_companies(df, companies):
    if len(companies) == 0:
        return df

    data = {
        CLMN_LEI : [],
        CLMN_COMPANY : [],
        CLMN_NAICS_SECTOR : [],
        CLMN_COUNTRY : [],
        CLMN_STOCK_EXCHANGE : [],
        CLMN_PERIOD_END : [],
        CLMN_MARKET_CAP: [],
        CLMN_FREE_FLOAT: [],
        CLMN_ALL_FACTS : [],
        CLMN_ALL_FACTS_PCT: [],
        CLMN_ESEF_FACTS : [],
        CLMN_ESEF_FACTS_PCT : [],
        CLMN_EXT_FACTS : [],
        CLMN_EXT_FACTS_PCT : [],
        CLMN_SHA1: [],
    }

    for l, c in companies.items():
        assert isinstance(c, company.Company)

        f = c.esef_filing

        assert isinstance(f, filing.ESEFFiling)

        data[CLMN_LEI].append(c.lei)
        data[CLMN_COMPANY].append(c.common_name)
        data[CLMN_NAICS_SECTOR].append(c.naics_sector)
        data[CLMN_COUNTRY].append(c.exchange_country)
        data[CLMN_STOCK_EXCHANGE].append(c.exchange_name)
        data[CLMN_PERIOD_END].append(f.period_end)
        data[CLMN_MARKET_CAP].append(c.company_market_cap)
        data[CLMN_FREE_FLOAT].append(c.free_float)

        all_facts = len(f.facts)

        data[CLMN_ALL_FACTS].append(all_facts)
        data[CLMN_ALL_FACTS_PCT].append(100.0)

        esef_facts = 0
        ext_facts = 0

        for fct in f.facts:
            assert isinstance(fct, filing.ESEFFact)

            if fct.is_extension:
                ext_facts += 1
            else:
                esef_facts += 1

        data[CLMN_ESEF_FACTS].append(esef_facts)
        data[CLMN_ESEF_FACTS_PCT].append(round(float(esef_facts) / float(all_facts) * 100, 2))
        data[CLMN_EXT_FACTS].append(ext_facts)
        data[CLMN_EXT_FACTS_PCT].append(round(float(ext_facts) / float(all_facts) * 100, 2))
        data[CLMN_SHA1].append(f.sha1)

    df_to_append = pd.DataFrame(data=data)

    df = pd.concat([df, df_to_append], ignore_index=True)

    df.to_excel(PATH_SF)

    return df

def group_by_country(df):
    assert isinstance(df, pd.DataFrame)

    countries = []

    s_countries = df["COUNTRY"] #get series with all countries

    for country in s_countries:
        try:
            countries.index(country)
        except ValueError:
            countries.append(country)

    df_countries = pd.DataFrame()

    for country in countries:
        df_filtered_by_country = df[df["COUNTRY"] == country] #dataframe mit allen Unternehmen eines Landes

        df_filtered_by_country.to_excel(PATH_COUNTRIES_DIR + "{}.xlsx".format(country.lower().replace(" ", "_"))) #speichern aller Unternehmen eines Landes
        
        df_filtered_by_country = df_filtered_by_country[["ALL_FACTS", "ALL_FACTS_PCT", "EXT_FACTS", "EXT_FACTS_PCT"]]

        df_filtered_by_country = df_filtered_by_country.describe().transpose()

        df_filtered_by_country.reset_index(inplace=True)

        df_filtered_by_country.rename(inplace=True, columns={"index" : "attribute"})

        df_filtered_by_country.insert(0, "country", [country, country, country, country])

        df_countries = pd.concat([df_countries, df_filtered_by_country])

    df_countries.reset_index(inplace=True, drop=True)

    df_countries.to_excel(PATH_COUNTRIES_DIR + "COUNTRIES.xlsx")

    return df_countries

def group_by_sector(df):
    assert isinstance(df, pd.DataFrame)

    sectors = []

    s_sectors = df["NAICS_SECTOR"] #get series with all sectors

    for sector in s_sectors:
        try:
            sectors.index(sector)
        except ValueError:
            sectors.append(sector)

    df_sectors = pd.DataFrame()

    for sector in sectors:
        df_filtered_by_sector = df[df["NAICS_SECTOR"] == sector] #dataframe mit allen Unternehmen eines Sektors

        df_filtered_by_sector.to_excel(PATH_SECTORS_DIR + "{}.xlsx".format(sector.lower().replace(" ", "_"))) #speichern aller Unternehmen eines Sektors
        
        df_filtered_by_sector = df_filtered_by_sector[["ALL_FACTS", "ALL_FACTS_PCT", "EXT_FACTS", "EXT_FACTS_PCT"]]

        df_filtered_by_sector = df_filtered_by_sector.describe().transpose()

        df_filtered_by_sector.reset_index(inplace=True)

        df_filtered_by_sector.rename(inplace=True, columns={"index" : "attribute"})

        df_filtered_by_sector.insert(0, "sector", [sector, sector, sector, sector])

        df_sectors = pd.concat([df_sectors, df_filtered_by_sector])

    df_sectors.reset_index(inplace=True, drop=True)

    df_sectors.to_excel(PATH_SECTORS_DIR + "SECTORS.xlsx")

    return df_sectors

def group_by_market_cap(df):
    assert isinstance(df, pd.DataFrame)

def show_graph_all(df):
    assert isinstance(df, pd.DataFrame)

    if df.empty:
        return

    df = df.sort_values("EXT_FACTS_PCT", ascending=False)

    df.plot(x="COMPANY", y="EXT_FACTS_PCT", kind="bar")
    plt.show()

def show_graph_countries(df):
    assert isinstance(df, pd.DataFrame)

    df = df.sort_values("mean", ascending=False)

    df.plot(x="COUNTRY", y="mean", kind="bar")
    plt.show()

def show_graph_sector(df):
    assert isinstance(df, pd.DataFrame)

    df = df.sort_values("EXT_FACTS_PCT", ascending=False)

    df.plot(x="COMPANY", y="EXT_FACTS_PCT", kind="bar")
    plt.show()
