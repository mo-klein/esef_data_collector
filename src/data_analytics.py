import os

import matplotlib.pyplot as plt

import pandas as pd

import numpy as np

import filing
import company

PATH_DATA_DIR = "./data/"
PATH_DF = PATH_DATA_DIR + "DATA.xlsx"

PATH_COUNTRIES_DIR = PATH_DATA_DIR + "/countries/"
PATH_SECTORS_DIR = PATH_DATA_DIR + "/sectors/"

#definiere Spalten des "Haupt-DataFrames"
CLMN_LEI = "LEI"
CLMN_COMPANY = "COMPANY"
CLMN_TRBC_SECTOR = "TRBC_SECTOR"
CLMN_COUNTRY = "COUNTRY"
CLMN_STOCK_EXCHANGE = "STOCK_EXCHANGE"
CLMN_INDEX = "INDEX"
CLMN_YEAR = "YEAR"
CLMN_MARKET_CAP = "MARKET_CAP"
CLMN_ALL_FACTS = "ALL_FACTS"
CLMN_ALL_FACTS_PCT = "ALL_FACTS_PCT"
CLMN_ESEF_FACTS = "ESEF_FACTS"
CLMN_ESEF_FACTS_PCT = "ESEF_FACTS_PCT"
CLMN_EXT_FACTS = "EXT_FACTS"
CLMN_EXT_FACTS_PCT = "EXT_FACTS_PCT"
CLMN_SHA1 = "SHA1"

def load_dataframe():
    if os.path.exists(PATH_DF):
        return pd.read_excel(PATH_DF, index_col=0)
    else:

        data = {
            CLMN_LEI : [],
            CLMN_COMPANY : [],
            CLMN_TRBC_SECTOR : [],
            CLMN_COUNTRY : [],
            CLMN_STOCK_EXCHANGE : [],
            CLMN_INDEX : [],
            CLMN_YEAR : [],
            CLMN_MARKET_CAP: [],
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
        CLMN_TRBC_SECTOR : [],
        CLMN_COUNTRY : [],
        CLMN_STOCK_EXCHANGE : [],
        CLMN_INDEX : [],
        CLMN_YEAR : [],
        CLMN_MARKET_CAP: [],
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

        for year, f in c.esef_filings.items():
            assert isinstance(f, filing.ESEFFiling)

            data[CLMN_LEI].append(c.lei)
            data[CLMN_COMPANY].append(c.common_name)
            data[CLMN_TRBC_SECTOR].append(c.trbc_economic_sector)
            data[CLMN_COUNTRY].append(c.exchange_country)
            data[CLMN_STOCK_EXCHANGE].append(c.exchange_name)
            data[CLMN_INDEX].append("")
            data[CLMN_YEAR].append(year)
            data[CLMN_MARKET_CAP].append("n/a")

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

    df.to_excel(PATH_DF)

    return df

def has_filing(df, sha1):
    assert isinstance(df, pd.DataFrame)

    if df.empty:
        return False

    df_filtered_by_sha1 = df["SHA1"].where(df["SHA1"] == sha1).dropna()

    return not df_filtered_by_sha1.empty

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

    s_sectors = df["TRBC_SECTOR"] #get series with all sectors

    for sector in s_sectors:
        try:
            sectors.index(sector)
        except ValueError:
            sectors.append(sector)

    df_sectors = pd.DataFrame()

    for sector in sectors:
        df_filtered_by_sector = df[df["TRBC_SECTOR"] == sector] #dataframe mit allen Unternehmen eines Sektors

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
