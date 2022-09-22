import argparse
from inspect import formatannotationrelativeto
import itertools
import logging
import os
import sys
from tokenize import group
from typing import Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
import patsy
import matplotlib as mpl
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler

import reporting
import eikon_database

PATH_SAMPLES_DIR = "./samples"

def main():
    logging.root.setLevel(100)

    arg_parser = argparse.ArgumentParser(description="Programm zur Erstellung von Stichproben für eine Masterarbeit zu dem Thema \"Die Erweiterungstaxonomie bei ESEF - kritische Würdigung und deskriptiver Befund\". Es extrahiert und inspeziert XBRL-Elemente aus Jahresfinanzberichten im ESEF-Format von kapitalmarktorientierten Unternehmen der Europäischen Union. Die so gewonnenen Informationen werden ergänzt durch Unternehmensdaten aus der Refinitiv Eikon Datenbank. Ferner können mit Hilfe des Programms die gesammelten Daten deskriptiv analysiert werden.")
    arg_group = arg_parser.add_mutually_exclusive_group()
    arg_parser.add_argument("sample_name", help="der Name des Samples")
    arg_group.add_argument("-ap", "--append", action="store_true", help="Wenn die Option gesetzt ist, werden alle Berichte, die noch nicht im Sample enthalten sind, dem Sample hinzugefügt.")
    arg_group.add_argument("-u", "--update", action="store_true", help="Wenn die Option gesetzt ist, werden alle Unternehmensdaten erneut aus Refinitiv Eikon heruntergeladen.")
    arg_group.add_argument("-an", "--analyze", action="store_true", help="Wenn die Option gesetzt ist, wird eine deskriptive Analyse zur Untersuchung des Auszeichnungsverhaltens der Unternehmen durchgeführt.")
    arg_group.add_argument("-r", "--regression", action="store_true", help="Wenn die Option gesetzt ist, wird eine Regressionsanalyse zur Ermittlung der Einflussfaktoren für eine erhöhte Verwendung von Erweiterungstaxonomieelementen druchgeführt.")

    args = arg_parser.parse_args()

    paths_sample_dirs = get_paths_sample_dirs(args.sample_name)

    for path in paths_sample_dirs:
        try:
            os.mkdir(path)
        except FileNotFoundError:
            print("\nBeim Erstellen des Ordner zum Speichern des Samples ist ein Fehler aufgetreten. Stellen Sie sich, dass der Ordner \"samples\" im Stammverzeichnis existiert.")
            _exit_with_error()
        except FileExistsError:
            pass
    
    (path_sample_dir,
        path_sample_esef_packages_dir,
        path_sample_reports_dir,
        path_sample_data_dir,
        path_sample_descriptive_analyses_dir,
        path_sample_regression_analyses_dir,
        *x) = paths_sample_dirs

    df = None
    columns = pd.array(["ESEF_PACKAGE_NAME", "LEI", "PERIOD_END", "ALL_TAGS", "PCT_ALL_TAGS", "ESEF_TAGS", "PCT_ESEF_TAGS", "EXT_TAGS", "PCT_EXT_TAGS", "SHA1", "ISIN", "COMPANY","SECTOR", "COUNTRY", "MARKET_CAP", "FREE_FLOAT", "AUDITOR", "AUDITOR_FEES", "EMPLOYEES", "FOUNDED", "ANALYSTS_FOLLOWING", "TOTAL_ASSETS", "TOTAL_DEBT", "INCOME", "TOTAL_ASSETS_T-1"])

    path_sample_data_data_file = "{}/{}.xlsx".format(path_sample_data_dir, args.sample_name)

    try:
        df = pd.read_excel(path_sample_data_data_file, sheet_name="DATA", index_col=0)
    except FileNotFoundError:
        df = pd.DataFrame(columns=columns)
    except PermissionError:
        print("\nStellen Sie sicher, dass die Datei \"{}\" nicht geöffnet ist.".format(path_sample_data_data_file))
        _exit_with_error()

    if args.analyze:
        _check_if_sample_is_empty(df, args.sample_name)

        _descriptive_analysis(df, args.sample_name, path_sample_descriptive_analyses_dir)

        _exit_gracefully()

    if args.regression:
        _check_if_sample_is_empty(df, args.sample_name)

        _regression_analysis(df, args.sample_name, path_sample_regression_analyses_dir)

        _exit_gracefully()

    if not eikon_database.setup():
        print("\nRefinitiv Eikon muss für die Verwendung des Programms gestartet sein. Falls Refinitv Eikon gestartet ist, überprüfen Sie auch den in der Datei \"config.yml\" hinterlegten App-Key.")
        _exit_with_error()

    if args.update:
        _check_if_sample_is_empty(df, args.sample_name)
        
        print("\nDie Unternehmensdaten der Stichprobe \"{}\" werden nun aktualisiert.".format(args.sample_name))

        columns_to_drop = ["ISIN", "COMPANY", "SECTOR", "COUNTRY", "MARKET_CAP", "FREE_FLOAT", "AUDITOR", "AUDITOR_FEES", "EMPLOYEES", "FOUNDED", "ANALYSTS_FOLLOWING", "TOTAL_ASSETS", "TOTAL_DEBT", "INCOME", "TOTAL_ASSETS_T-1"]
        df = df.drop(columns=columns_to_drop)

        reports = df.to_numpy().tolist()

        eikon_database.get_company_data(reports)

        df = pd.DataFrame(reports, columns=columns)

        # df = pd.DataFrame(columns=columns)

        # df = pd.concat([df, df_reports], ignore_index=True)

        df.to_excel(path_sample_data_data_file, sheet_name="DATA")

        print("\nDie Unternehmensdaten der Stichprobe \"{}\" wurden aktualisiert.".format(args.sample_name))
        print("\nStichprobe gespeichert in \"{}\".".format(path_sample_data_data_file))
        
        _exit_gracefully()

    if df.empty or args.append:
        print("\nESEF-Pakete werden nun geladen.")

        reports = reporting.load_reports(df["SHA1"], path_sample_esef_packages_dir, path_sample_reports_dir)

        eikon_database.get_company_data(reports)

        df_reports = pd.DataFrame(reports, columns=columns)

        df = pd.concat([df, df_reports], ignore_index=True)

        df.to_excel(path_sample_data_data_file, sheet_name="DATA")

        print("\nEs wurde(n) {} Bericht(e) geladen.".format(len(reports)))

        if(len(reports) > 0):
            print("\nStichprobe gespeichert in \"{}\".".format(path_sample_data_data_file))
    else:
        print("\nDie Stichprobe \"{}\" ist bereits vorhanden. Bitte wählen Sie einen anderen Namen.\nHinweis: Möchten Sie Elemente zur Stichprobe hinzufügen, verwenden Sie bitte die Option -ap oder --append.".format(args.sample_name))

    print("")

def get_paths_sample_dirs(sample_name: str) -> Tuple[str, str, str, str, str, str, str, str, str, str]:
    path_sample_dir = PATH_SAMPLES_DIR + "/" + sample_name
    path_sample_descriptive_analysis_dir = path_sample_dir + "/descriptive_analyses"
    path_sample_regression_analysis_dir = path_sample_dir + "/regression_analyses"

    return (
        path_sample_dir,
        path_sample_dir + "/esef_packages",
        path_sample_dir + "/reports",
        path_sample_dir + "/data",
        path_sample_descriptive_analysis_dir,
        path_sample_descriptive_analysis_dir + "/country_analysis",
        path_sample_descriptive_analysis_dir + "/sector_analysis",
        path_sample_descriptive_analysis_dir + "/market_cap_analysis",
        path_sample_descriptive_analysis_dir + "/free_float_analysis",
        path_sample_descriptive_analysis_dir + "/auditor_analysis",
        path_sample_regression_analysis_dir,
        path_sample_regression_analysis_dir + "/model01",
        path_sample_regression_analysis_dir + "/model02",
        path_sample_regression_analysis_dir + "/model03",
        path_sample_regression_analysis_dir + "/model04",
        path_sample_regression_analysis_dir + "/model05",
        path_sample_regression_analysis_dir + "/model06",
        path_sample_regression_analysis_dir + "/model07",
        path_sample_regression_analysis_dir + "/model08",
        path_sample_regression_analysis_dir + "/model09",
        path_sample_regression_analysis_dir + "/model10"
    )

def _check_if_sample_is_empty(df: pd.DataFrame, sample_name: str):
    if df.empty:
            print("\nDie Stichprobe \"{}\" ist nicht vorhanden.".format(sample_name))
            _exit_with_error()

def _exit_gracefully():
        print("")
        sys.exit(0)

def _exit_with_error():
        print("")       
        sys.exit(1)

def _descriptive_analysis(df: pd.DataFrame, sample_name: str, path_sample_descriptive_analyses_dir: str):

    paths_sample_dirs = get_paths_sample_dirs(sample_name)

    path_s_d_a_summary_file = paths_sample_dirs[4] + "/{}_summary.xlsx".format(sample_name)

    df = df.dropna(subset=["COUNTRY", "SECTOR", "MARKET_CAP", "FREE_FLOAT", "AUDITOR"])

    with pd.ExcelWriter(path_s_d_a_summary_file) as writer:
        df.to_excel(writer, sheet_name="DATA")
        df["ALL_TAGS"].describe().to_excel(writer, sheet_name="ALL_TAGS")
        df["PCT_EXT_TAGS"].describe().to_excel(writer, sheet_name="PCT_EXT_TAGS")

    #######################
    ### Ländervergleich ###
    #######################

    path_s_d_a_ca_dir = paths_sample_dirs[5]

    path_s_d_a_ca_summary_file = path_s_d_a_ca_dir + "/ca_{}_summary.xlsx".format(sample_name)
    path_s_d_a_ca_plt_all_tags_file = path_s_d_a_ca_dir + "/ca_{}_plt_all_tags.pdf".format(sample_name)
    path_s_d_a_ca_plt_pct_ext_tags_file = path_s_d_a_ca_dir + "/ca_{}_plt_pct_ext_tags.pdf".format(sample_name)

    grouped_by_country = df.groupby(df["COUNTRY"])

    with pd.ExcelWriter(path_s_d_a_ca_summary_file) as writer:
        grouped_by_country["ALL_TAGS"].describe().to_excel(writer, sheet_name="ALL_TAGS")
        grouped_by_country["PCT_EXT_TAGS"].describe().to_excel(writer, sheet_name="PCT_EXT_TAGS")

    grouped_by_country["ALL_TAGS"].mean().plot.bar(xlabel="Land", ylabel="\u00D8 Anzahl verwendeter Tags")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_ca_plt_all_tags_file)
    plt.close(fig)

    grouped_by_country["PCT_EXT_TAGS"].mean().plot.bar(xlabel="Land", ylabel="\u00D8 Anteil an ETEs (%)")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_ca_plt_pct_ext_tags_file)
    plt.close(fig)

    #######################
    ### Sektorenanalyse ###
    #######################

    path_s_d_a_sa_dir = paths_sample_dirs[6]

    path_s_d_a_sa_summary_file = path_s_d_a_sa_dir + "/sa_{}_summary.xlsx".format(sample_name)
    path_s_d_a_sa_plt_all_tags_file = path_s_d_a_sa_dir + "/sa_{}_plt_all_tags.pdf".format(sample_name)
    path_s_d_a_sa_plt_pct_ext_tags_file = path_s_d_a_sa_dir + "/sa_{}_plt_pct_ext_tags.pdf".format(sample_name)

    grouped_by_naics_sector = df.groupby(df["SECTOR"])

    with pd.ExcelWriter(path_s_d_a_sa_summary_file) as writer:
        grouped_by_naics_sector["ALL_TAGS"].describe().sort_values("mean", ascending=False).to_excel(writer, sheet_name="ALL_TAGS")
        grouped_by_naics_sector["PCT_EXT_TAGS"].describe().sort_values("mean", ascending=False).to_excel(writer, sheet_name="PCT_EXT_TAGS")

    grouped_by_naics_sector["ALL_TAGS"].mean().sort_values(ascending=False).plot.bar(xlabel="Sektor (TRBC)", ylabel="\u00D8 Anzahl verwendeter Tags")
    plt.xticks(fontsize=6, rotation=20, ha="right")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_sa_plt_all_tags_file)
    plt.close(fig)

    grouped_by_naics_sector["PCT_EXT_TAGS"].mean().sort_values(ascending=False).plot.bar(xlabel="Sektor (TRBC)", ylabel="\u00D8 Anteil an ETEs (%)")
    plt.xticks(fontsize=6, rotation=20, ha="right")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_sa_plt_pct_ext_tags_file)
    plt.close(fig)

    ############################################
    ### Analyse nach Unternehmensgröße (MVA) ###
    ############################################

    path_s_d_a_mca_dir = paths_sample_dirs[7]

    path_s_d_a_mca_data_file = path_s_d_a_mca_dir + "/mca_{}.xlsx".format(sample_name)
    path_s_d_a_mca_summary_file = path_s_d_a_mca_dir + "/mca_{}_summary.xlsx".format(sample_name)
    path_s_d_a_mca_plt_all_tags_file = path_s_d_a_mca_dir + "/mca_{}_plt_all_tags.pdf".format(sample_name)
    path_s_d_a_mca_plt_pct_ext_tags_file = path_s_d_a_mca_dir + "/mca_{}_plt_pct_ext_tags.pdf".format(sample_name)

    df_market_cap = df.copy(deep=True)
    df_market_cap["MARKET_CAP_CAT"] = df_market_cap["MARKET_CAP"].transform(lambda x: pd.qcut(x, 5))

    df_market_cap.to_excel(path_s_d_a_mca_data_file, sheet_name="DATA")

    grouped_by_market_cap = df_market_cap.groupby(df_market_cap["MARKET_CAP_CAT"])

    with pd.ExcelWriter(path_s_d_a_mca_summary_file) as writer:
        grouped_by_market_cap["ALL_TAGS"].describe().to_excel(writer, sheet_name="ALL_TAGS")
        grouped_by_market_cap["PCT_EXT_TAGS"].describe().to_excel(writer, sheet_name="PCT_EXT_TAGS")

    grouped_by_market_cap["ALL_TAGS"].mean().plot.bar(xlabel="Marktkapitalisierung zum Abschlussstichtag (Mio. EUR)", ylabel="\u00D8 Anzahl verwendeter Tags")
    plt.xticks(fontsize=6, rotation=20, ha="right")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_mca_plt_all_tags_file)
    plt.close(fig)

    grouped_by_market_cap["PCT_EXT_TAGS"].mean().plot.bar(xlabel="Marktkapitalisierung zum Abschlussstichtag (Mio. EUR)", ylabel="\u00D8 Anteil an ETEs (%)")
    plt.xticks(fontsize=6, rotation=20, ha="right")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_mca_plt_pct_ext_tags_file)
    plt.close(fig)

    ###############################
    ### Analyse nach Free Float ###
    ###############################

    path_s_d_a_ffa_dir = paths_sample_dirs[8]

    path_s_d_a_ffa_data_file = path_s_d_a_ffa_dir + "/ffa_{}.xlsx".format(sample_name)
    path_s_d_a_ffa_summary_file = path_s_d_a_ffa_dir + "/ffa_{}_summary.xlsx".format(sample_name)
    path_s_d_a_ffa_plt_all_tags_file = path_s_d_a_ffa_dir + "/ffa_{}_plt_all_tags.pdf".format(sample_name)
    path_s_d_a_ffa_plt_pct_ext_tags_file = path_s_d_a_ffa_dir + "/ffa_{}_plt_pct_ext_tags.pdf".format(sample_name)

    df_free_float = df.copy(deep=True)
    df_free_float["FREE_FLOAT_CAT"] = df_free_float["FREE_FLOAT"].transform(lambda x: pd.qcut(x, 5))

    df_free_float.to_excel(path_s_d_a_ffa_data_file, sheet_name="DATA")

    grouped_by_free_float = df_free_float.groupby(df_free_float["FREE_FLOAT_CAT"])

    with pd.ExcelWriter(path_s_d_a_ffa_summary_file) as writer:
        grouped_by_free_float["ALL_TAGS"].describe().to_excel(writer, sheet_name="ALL_TAGS")
        grouped_by_free_float["PCT_EXT_TAGS"].describe().to_excel(writer, sheet_name="PCT_EXT_TAGS")

    grouped_by_free_float["ALL_TAGS"].mean().plot.bar(xlabel="Streubesitz zum Abschlussstichtag (%)", ylabel="\u00D8 Anzahl verwendeter Tags")
    plt.xticks(fontsize=6, rotation=20, ha="right")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_ffa_plt_all_tags_file)
    plt.close(fig)

    grouped_by_free_float["PCT_EXT_TAGS"].mean().plot.bar(xlabel="Streubesitz zum Abschlussstichtag (%)", ylabel="\u00D8 Anteil an ETEs (%)")
    plt.xticks(fontsize=6, rotation=20, ha="right")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_ffa_plt_pct_ext_tags_file)
    plt.close(fig)

    ###########################
    ### Analyse nach Prüfer ###
    ###########################

    path_s_d_a_aa_dir = paths_sample_dirs[9]

    path_s_d_a_aa_summary_file = path_s_d_a_aa_dir + "/aa_{}_summary.xlsx".format(sample_name)
    path_s_d_a_aa_plt_all_tags_file = path_s_d_a_aa_dir + "/aa_{}_plt_all_tags.pdf".format(sample_name)
    path_s_d_a_aa_plt_pct_ext_tags_file = path_s_d_a_aa_dir + "/aa_{}_plt_pct_ext_tags.pdf".format(sample_name)
    path_s_d_a_aa_plt_all_tags_alt1_file = path_s_d_a_aa_dir + "/aa_{}_plt_all_tags_alt1.pdf".format(sample_name)
    path_s_d_a_aa_plt_pct_ext_tags_alt1_file = path_s_d_a_aa_dir + "/aa_{}_plt_pct_ext_tags_alt1.pdf".format(sample_name)

    big4 = ["EY", "Deloitte", "PWC", "KPMG"]
    column_auditor_agg = pd.Series(np.where(df["AUDITOR"].isin(big4), df["AUDITOR"], "Non Big-4"))
    df = df.assign(AUDITOR_AGG=column_auditor_agg.values)

    grouped_by_auditor = df.groupby(df["AUDITOR"])
    grouped_by_auditor_alt1 = df.groupby(df["AUDITOR_AGG"])

    with pd.ExcelWriter(path_s_d_a_aa_summary_file) as writer:
        grouped_by_auditor["ALL_TAGS"].describe().to_excel(writer, sheet_name="ALL_TAGS")
        grouped_by_auditor["PCT_EXT_TAGS"].describe().to_excel(writer, sheet_name="PCT_EXT_TAGS")
        grouped_by_auditor_alt1["ALL_TAGS"].describe().to_excel(writer, sheet_name="ALL_TAGS_ALT1")
        grouped_by_auditor_alt1["PCT_EXT_TAGS"].describe().to_excel(writer, sheet_name="PCT_EXT_TAGS_ALT1")

    grouped_by_auditor["ALL_TAGS"].mean().plot.bar(xlabel="Abschlussprüfer", ylabel="\u00D8 Anzahl verwendeter Tags")
    plt.xticks(fontsize=6, rotation=20, ha="right")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_aa_plt_all_tags_file)
    plt.close(fig)

    grouped_by_auditor["PCT_EXT_TAGS"].mean().plot.bar(xlabel="Abschlussprüfer", ylabel="\u00D8 Anteil an ETEs (%)")
    plt.xticks(fontsize=6, rotation=20, ha="right")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_aa_plt_pct_ext_tags_file)
    plt.close(fig)

    grouped_by_auditor_alt1["ALL_TAGS"].mean().plot.bar(xlabel="Abschlussprüfer", ylabel="\u00D8 Anzahl verwendeter Tags")
    plt.xticks(fontsize=6, rotation=20, ha="right")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_aa_plt_all_tags_alt1_file)
    plt.close(fig)

    grouped_by_auditor_alt1["PCT_EXT_TAGS"].mean().plot.bar(xlabel="Abschlussprüfer", ylabel="\u00D8 Anteil an ETEs (%)")
    plt.xticks(fontsize=6, rotation=20, ha="right")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_aa_plt_pct_ext_tags_alt1_file)
    plt.close(fig)

def _regression_analysis(df: pd.DataFrame, sample_name: str, path_sample_regression_analyses_dir: str):

    paths_sample_dirs = get_paths_sample_dirs(sample_name)

    df = _prepare_data(df)

    #_hist_exo_vars(df)

    ###############
    ### Model 1 ###
    ###############

    path_s_r_a_m1_dir = paths_sample_dirs[11]

    # Erstellt auf Basis der Regressionsgleichung einen Vektor, der die abhängige Variable enthält und eine Matrix, die das Interzept und die unabhängigen Variable enthält. 
    y, X = patsy.dmatrices("log_PCT_EXT_TAGS ~ log_ALL_TAGS + log_MARKET_CAP + IS_FIN + OLD_COMPANY + log_FREE_FLOAT + log_DEBT + log_ROA + COUNTRY", data=df, return_type="dataframe")

    _run_model(sample_name, "m1", y, X, path_s_r_a_m1_dir)

    ###############
    ### Model 2 ###
    ###############

    path_s_r_a_m2_dir = paths_sample_dirs[12]

    # Erstellt auf Basis der Regressionsgleichung einen Vektor, der die abhängige Variable enthält und eine Matrix, die das Interzept und die unabhängigen Variable enthält. 
    y, X = patsy.dmatrices("log_PCT_EXT_TAGS ~ log_ALL_TAGS + log_TOTAL_ASSETS + IS_FIN + OLD_COMPANY + log_FREE_FLOAT + log_DEBT + log_ROA + COUNTRY", data=df, return_type="dataframe")

    _run_model(sample_name, "m2", y, X, path_s_r_a_m2_dir)

    ###############
    ### Model 3 ###
    ###############

    path_s_r_a_m3_dir = paths_sample_dirs[13]

    # Erstellt auf Basis der Regressionsgleichung einen Vektor, der die abhängige Variable enthält und eine Matrix, die das Interzept und die unabhängigen Variable enthält. 
    y, X = patsy.dmatrices("log_PCT_EXT_TAGS ~ log_ALL_TAGS + log_MARKET_CAP + SECTOR + OLD_COMPANY + log_FREE_FLOAT + log_DEBT + log_ROA + COUNTRY", data=df, return_type="dataframe")

    _run_model(sample_name, "m3", y, X, path_s_r_a_m3_dir)

    ###############
    ### Model 4 ###
    ###############

    path_s_r_a_m4_dir = paths_sample_dirs[14]

    # Erstellt auf Basis der Regressionsgleichung einen Vektor, der die abhängige Variable enthält und eine Matrix, die das Interzept und die unabhängigen Variable enthält. 
    y, X = patsy.dmatrices("log_PCT_EXT_TAGS ~ log_ALL_TAGS + log_TOTAL_ASSETS + SECTOR + OLD_COMPANY + log_FREE_FLOAT + log_DEBT + log_ROA + COUNTRY", data=df, return_type="dataframe")

    _run_model(sample_name, "m4", y, X, path_s_r_a_m4_dir)

    ###############
    ### Model 5 ###
    ###############

    path_s_r_a_m5_dir = paths_sample_dirs[15]

    # Erstellt auf Basis der Regressionsgleichung einen Vektor, der die abhängige Variable enthält und eine Matrix, die das Interzept und die unabhängigen Variable enthält. 
    y, X = patsy.dmatrices("log_PCT_EXT_TAGS ~ log_ALL_TAGS + log_MARKET_CAP + SECTOR + OLD_COMPANY + log_FREE_FLOAT + log_DEBT + log_ROA + COUNTRY + AUDITOR_AGG", data=df, return_type="dataframe")

    _run_model(sample_name, "m5", y, X, path_s_r_a_m5_dir)

    ###############
    ### Model 6 ###
    ###############

    path_s_r_a_m6_dir = paths_sample_dirs[16]

    # Erstellt auf Basis der Regressionsgleichung einen Vektor, der die abhängige Variable enthält und eine Matrix, die das Interzept und die unabhängigen Variable enthält. 
    y, X = patsy.dmatrices("log_PCT_EXT_TAGS ~ log_ALL_TAGS + log_TOTAL_ASSETS + SECTOR + OLD_COMPANY + log_FREE_FLOAT + log_DEBT + log_ROA + COUNTRY + AUDITOR_AGG", data=df, return_type="dataframe")

    _run_model(sample_name, "m6", y, X, path_s_r_a_m6_dir)

    ###############
    ### Model 7 ###
    ###############

    path_s_r_a_m7_dir = paths_sample_dirs[17]

    # Erstellt auf Basis der Regressionsgleichung einen Vektor, der die abhängige Variable enthält und eine Matrix, die das Interzept und die unabhängigen Variable enthält. 
    y, X = patsy.dmatrices("log_PCT_EXT_TAGS ~ log_ALL_TAGS + log_MARKET_CAP + IS_FIN + OLD_COMPANY + log_FREE_FLOAT + log_DEBT + log_ROA", data=df, return_type="dataframe")

    _run_model(sample_name, "m7", y, X, path_s_r_a_m7_dir)

    ###############
    ### Model 8 ###
    ###############

    path_s_r_a_m8_dir = paths_sample_dirs[18]

    # Erstellt auf Basis der Regressionsgleichung einen Vektor, der die abhängige Variable enthält und eine Matrix, die das Interzept und die unabhängigen Variable enthält. 
    y, X = patsy.dmatrices("log_PCT_EXT_TAGS ~ log_ALL_TAGS + log_TOTAL_ASSETS + IS_FIN + OLD_COMPANY + log_FREE_FLOAT + log_DEBT + log_ROA", data=df, return_type="dataframe")

    _run_model(sample_name, "m8", y, X, path_s_r_a_m8_dir)

    ###############
    ### Model 9 ###
    ###############

    path_s_r_a_m9_dir = paths_sample_dirs[19]

    # Erstellt auf Basis der Regressionsgleichung einen Vektor, der die abhängige Variable enthält und eine Matrix, die das Interzept und die unabhängigen Variable enthält. 
    y, X = patsy.dmatrices("log_PCT_EXT_TAGS ~ log_ALL_TAGS + log_MARKET_CAP + SECTOR + OLD_COMPANY + log_FREE_FLOAT + log_DEBT + log_ROA", data=df, return_type="dataframe")

    _run_model(sample_name, "m9", y, X, path_s_r_a_m9_dir)

    ###############
    ### Model 10 ###
    ###############

    path_s_r_a_m10_dir = paths_sample_dirs[20]

    # Erstellt auf Basis der Regressionsgleichung einen Vektor, der die abhängige Variable enthält und eine Matrix, die das Interzept und die unabhängigen Variable enthält. 
    y, X = patsy.dmatrices("log_PCT_EXT_TAGS ~ log_ALL_TAGS + log_TOTAL_ASSETS + SECTOR + OLD_COMPANY + log_FREE_FLOAT + log_DEBT + log_ROA", data=df, return_type="dataframe")

    _run_model(sample_name, "m10", y, X, path_s_r_a_m10_dir)

    # Kontrolldiagramm: Teilregression PCT_EXT_TAGS ~ log_MARKET_CAP
    # figure = sm.graphics.plot_regress_exog(res, "log_MARKET_CAP")
    # plt.show()

def _prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    # Entfernt alle Einträge, die leere Zellen enthalten.
    # df = df.dropna()
    df = df.dropna(subset=["COUNTRY", "SECTOR", "MARKET_CAP", "FREE_FLOAT", "AUDITOR", "FOUNDED", "TOTAL_ASSETS", "TOTAL_DEBT", "INCOME", "TOTAL_ASSETS_T-1"])

    # Fügt eine neue Spalte hinzu, welche den log-Wert der Spalte MARKET_CAP enthält. Die Verteilung der Daten der Variable MARKET_CAP ist rechtsschief. Ich habe gelesen, dass die Logarithmesierung die Verteilung normalisieren kann.
    df = df.assign(log_PCT_EXT_TAGS=pd.Series(np.log(df["PCT_EXT_TAGS"] + 1)).values)
    df = df.assign(log_MARKET_CAP=pd.Series(np.log(df["MARKET_CAP"])).values)
    df = df.assign(log_TOTAL_ASSETS=pd.Series(np.log(df["TOTAL_ASSETS"])).values)
    df = df.assign(log_AUDITOR_FEES=pd.Series(np.log(df["AUDITOR_FEES"])).values)
    df = df.assign(log_EMPLOYEES=pd.Series(np.log(df["EMPLOYEES"] + 1)).values)
    df = df.assign(log_ALL_TAGS=pd.Series(np.log(df["ALL_TAGS"] + 1)).values)
    df = df.assign(log_FREE_FLOAT=pd.Series(np.log(df["FREE_FLOAT"] + 1)).values)
    df = df.assign(log_ANALYSTS_FOLLOWING=pd.Series(np.log(df["ANALYSTS_FOLLOWING"] + 1)).values)
    
    #df["log_PCT_EXT_TAGS"] = np.log(df["PCT_EXT_TAGS"] + 1)
    #df["log_MARKET_CAP"] = np.log(df["MARKET_CAP"])
    #df["log_TOTAL_ASSETS"] = np.log(df["TOTAL_ASSETS"])
    #df["log_AUDITOR_FEES"] = np.log(df["AUDITOR_FEES"])
    #df["log_EMPLOYEES"] = np.log(df["EMPLOYEES"] + 1)
    #df["log_ALL_TAGS"] = np.log(df["ALL_TAGS"] + 1)
    #df["log_FREE_FLOAT"] = np.log(df["FREE_FLOAT"] + 1)
    #df["log_ANALYSTS_FOLLOWING"] = np.log(df["ANALYSTS_FOLLOWING"] + 1)

    # Fügt eine neue Spalte hinzu, welche die Dummy-Variable "IS_FIN" darstellt. Für jeden Eintrag, der zu dem Sektor "Finance and Insurance" oder "Real Estate and Rental and Leasing" gehört, wird die Dummy-Variable auf "Wahr" gesetzt.
    df = df.assign(IS_FIN=pd.Series(df["SECTOR"].isin(["Financials", "Real Estate"])).values)
    #df["IS_FIN"] = (
        #(df["SECTOR"] == "Financials")
    #)

    df = df.assign(OLD_COMPANY=pd.Series((2022 - df["FOUNDED"] > 10)).values)
    #df["OLD_COMPANY"] = (
        #(2022 - df["FOUNDED"] > 10)
    #)

    df = df.assign(ROA=pd.Series((df["INCOME"] / (0.5 * (df["TOTAL_ASSETS_T-1"] + df["TOTAL_ASSETS"])))).values)
    #df["ROA"] = (
        #df["INCOME"] / df["TOTAL_ASSETS_T-1"]
    #)

    df = df.assign(DEBT=pd.Series((df["TOTAL_DEBT"] / df["TOTAL_ASSETS"])).values)
    #df["DEBT"] = (
        #df["TOTAL_DEBT"] / df["TOTAL_ASSETS"]
    #)

    df = df.assign(log_ROA=pd.Series(np.log(df["ROA"] + 1)).values)
    #df["log_ROA"] = np.log(df["ROA"] + 1)

    df = df.assign(log_DEBT=pd.Series(np.log(df["DEBT"] + 1)).values)
    #df["log_DEBT"] = np.log(df["DEBT"] + 1)

    big4 = ["EY", "Deloitte", "PWC", "KPMG"]
    column_auditor_agg = pd.Series(np.where(df["AUDITOR"].isin(big4), df["AUDITOR"], "Non Big-4"))
    df = df.assign(AUDITOR_AGG=column_auditor_agg.values)

    return df

def _hist_exo_vars(df: pd.DataFrame):
    # Kontrolldiagramm: Verteilung der Variable log_MARKET_CAP
    df.hist("log_MARKET_CAP", figsize=(8,5))
    plt.tight_layout()
    plt.title("log(Market Cap)")
    plt.show()

    df.hist("log_PCT_EXT_TAGS", figsize=(8,5))
    plt.tight_layout()
    plt.title("log(PCT EXT TAGS)")
    plt.show()


def _run_model(sample_name:str, model_name: str, y: pd.DataFrame, X: pd.DataFrame, dir: str):
    # Hinterlegung von Dateipfaden zur Speicherung der abhängigen Variable, der unabhängigen Variablen und des Ergebnisses
    path_s_r_a_pearson_file = dir + "/{}_{}_pearson.xlsx".format(model_name, sample_name)
    path_s_r_a_model_file = dir + "/{}_{}_model.xlsx".format(model_name, sample_name)
    path_s_r_a_summary_html_file = dir + "/{}_{}_summary.html".format(model_name, sample_name)
    path_s_r_a_summary_text_file = dir + "/{}_{}_summary.txt".format(model_name, sample_name)

    X.corr().to_excel(path_s_r_a_pearson_file, sheet_name="PEARSON_CORR")

    # Speichert den Vektor und die Matrix jeweils in einer Excel-Datei.
    with pd.ExcelWriter(path_s_r_a_model_file) as writer:
        y.to_excel(writer, sheet_name="y")
        X.to_excel(writer, sheet_name="X")

    # Erzeugt ein OLS-Objekt.
    mod = sm.OLS(y, X)

    # Führt die Schätzung der Regressionsgeraden durch.
    res = mod.fit()

    # Speichert das Ergebnis der Regression in der Variable "summary"
    summary = res.summary()

    # Speicher das Ergebnis als HTML-Datei
    with open(path_s_r_a_summary_html_file, "w") as file:
        file.write(summary.as_html())

    # Speicher das Ergebnis als Text-Datei
    with open(path_s_r_a_summary_text_file, "w") as file:
        file.write(summary.as_text())

    # Ausgabe des Ergebnisses auf der Konsole
    print(summary)

if __name__ == "__main__":
    main()