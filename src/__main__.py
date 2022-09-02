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
    columns = pd.array(["ESEF_PACKAGE_NAME", "LEI", "PERIOD_END", "ALL_TAGS", "PCT_ALL_TAGS", "ESEF_TAGS", "PCT_ESEF_TAGS", "EXT_TAGS", "PCT_EXT_TAGS", "SHA1", "ISIN", "COMPANY","NAICS_SECTOR", "COUNTRY", "MARKET_CAP", "FREE_FLOAT", "AUDITOR", "AUDITOR_FEES", "EMPLOYEES", "FOUNDED", "ANALYSTS_FOLLOWING", "TOTAL_ASSETS", "TOTAL_DEBT", "INCOME", "TOTAL_ASSETS_T-1"])

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

        columns_to_drop = ["ISIN", "COMPANY", "NAICS_SECTOR", "COUNTRY", "MARKET_CAP", "FREE_FLOAT", "AUDITOR", "AUDITOR_FEES", "EMPLOYEES", "FOUNDED", "ANALYSTS_FOLLOWING", "TOTAL_ASSETS", "TOTAL_DEBT", "INCOME", "TOTAL_ASSETS_T-1"]
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
        path_sample_dir + "/regression_analyses"
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

    #######################
    ### Ländervergleich ###
    #######################

    path_s_d_a_ca_dir = paths_sample_dirs[5]

    path_s_d_a_ca_summary_file = path_s_d_a_ca_dir + "/ca_summary.xlsx"
    path_s_d_a_ca_plt_all_tags_file = path_s_d_a_ca_dir + "/ca_plt_all_tags.pdf"
    path_s_d_a_ca_plt_pct_ext_tags_file = path_s_d_a_ca_dir + "/ca_plt_pct_ext_tags.pdf"

    path_s_d_a_ca_without_fin_data_file = path_s_d_a_ca_dir + "/ca_{}_without_fin.xlsx".format(sample_name)
    path_s_d_a_ca_without_fin_summary_file = path_s_d_a_ca_dir + "/ca_without_fin_summary.xlsx"
    path_s_d_a_ca_without_fin_plt_all_tags_file = path_s_d_a_ca_dir + "/ca_without_fin_plt_all_tags.pdf"
    path_s_d_a_ca_without_fin_plt_pct_ext_tags_file = path_s_d_a_ca_dir + "/ca_without_fin_plt_pct_ext_tags.pdf"

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

    df_without_fin = df.where((df["NAICS_SECTOR"] != "Finance and Insurance") & (df["NAICS_SECTOR"] != "Real Estate and Rental and Leasing"))
    df_without_fin.to_excel(path_s_d_a_ca_without_fin_data_file, sheet_name="DATA")

    grouped_by_country_without_fin = df_without_fin.groupby(df["COUNTRY"])

    with pd.ExcelWriter(path_s_d_a_ca_without_fin_summary_file) as writer:
        grouped_by_country_without_fin["ALL_TAGS"].describe().to_excel(writer, sheet_name="ALL_TAGS")
        grouped_by_country_without_fin["PCT_EXT_TAGS"].describe().to_excel(writer, sheet_name="PCT_EXT_TAGS")

    grouped_by_country_without_fin["ALL_TAGS"].mean().plot.bar(xlabel="Land", ylabel="\u00D8 Anzahl verwendeter Tags")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_ca_without_fin_plt_all_tags_file)
    plt.close(fig)

    grouped_by_country_without_fin["PCT_EXT_TAGS"].mean().plot.bar(xlabel="Land", ylabel="\u00D8 Anteil an ETEs (%)")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_ca_without_fin_plt_pct_ext_tags_file)
    plt.close(fig)

    #######################
    ### Sektorenanalyse ###
    #######################

    path_s_d_a_sa_dir = paths_sample_dirs[6]

    path_s_d_a_sa_summary_file = path_s_d_a_sa_dir + "/sa_summary.xlsx"
    path_s_d_a_sa_plt_all_tags_file = path_s_d_a_sa_dir + "/sa_plt_all_tags.pdf"
    path_s_d_a_sa_plt_pct_ext_tags_file = path_s_d_a_sa_dir + "/sa_plt_pct_ext_tags.pdf"

    grouped_by_naics_sector = df.groupby(df["NAICS_SECTOR"])

    with pd.ExcelWriter(path_s_d_a_sa_summary_file) as writer:
        grouped_by_naics_sector["ALL_TAGS"].describe().sort_values("mean", ascending=False).to_excel(writer, sheet_name="ALL_TAGS")
        grouped_by_naics_sector["PCT_EXT_TAGS"].describe().sort_values("mean", ascending=False).to_excel(writer, sheet_name="PCT_EXT_TAGS")

    grouped_by_naics_sector["ALL_TAGS"].mean().sort_values(ascending=False).plot.bar(xlabel="Sektor (NAICS)", ylabel="\u00D8 Anzahl verwendeter Tags")
    plt.xticks(fontsize=6, rotation=20, ha="right")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(path_s_d_a_sa_plt_all_tags_file)
    plt.close(fig)

    grouped_by_naics_sector["PCT_EXT_TAGS"].mean().sort_values(ascending=False).plot.bar(xlabel="Sektor (NAICS)", ylabel="\u00D8 Anteil an ETEs (%)")
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
    path_s_d_a_mca_summary_file = path_s_d_a_mca_dir + "/mca_summary.xlsx"
    path_s_d_a_mca_plt_all_tags_file = path_s_d_a_mca_dir + "/mca_plt_all_tags.pdf"
    path_s_d_a_mca_plt_pct_ext_tags_file = path_s_d_a_mca_dir + "/mca_plt_pct_ext_tags.pdf"

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
    path_s_d_a_ffa_summary_file = path_s_d_a_ffa_dir + "/ffa_summary.xlsx"
    path_s_d_a_ffa_plt_all_tags_file = path_s_d_a_ffa_dir + "/ffa_plt_all_tags.pdf"
    path_s_d_a_ffa_plt_pct_ext_tags_file = path_s_d_a_ffa_dir + "/ffa_plt_pct_ext_tags.pdf"

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

    path_s_d_a_aa_summary_file = path_s_d_a_aa_dir + "/aa_summary.xlsx"
    path_s_d_a_aa_plt_all_tags_file = path_s_d_a_aa_dir + "/aa_plt_all_tags.pdf"
    path_s_d_a_aa_plt_pct_ext_tags_file = path_s_d_a_aa_dir + "/aa_plt_pct_ext_tags.pdf"

    grouped_by_auditor = df.groupby(df["AUDITOR"])

    with pd.ExcelWriter(path_s_d_a_aa_summary_file) as writer:
        grouped_by_auditor["ALL_TAGS"].describe().to_excel(writer, sheet_name="ALL_TAGS")
        grouped_by_auditor["PCT_EXT_TAGS"].describe().to_excel(writer, sheet_name="PCT_EXT_TAGS")

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

def _regression_analysis(df: pd.DataFrame, sample_name: str, path_sample_regression_analyses_dir: str):

    # Hinterlegung von Dateipfaden zur Speicherung der abhängigen Variable, der unabhängigen Variablen und des Ergebnisses
    path_sample_regression_analyses_y_file = path_sample_regression_analyses_dir + "/{}_y.xlsx".format(sample_name)
    path_sample_regression_analyses_X_file = path_sample_regression_analyses_dir + "/{}_X.xlsx".format(sample_name)
    path_sample_regression_analyses_summary_file = path_sample_regression_analyses_dir + "/{}_summary.html".format(sample_name)

    # Liste mit Spalten, die für die Regression benötigt werden.
    vars_of_intrest = ["PCT_EXT_TAGS", "NAICS_SECTOR", "MARKET_CAP"]

    # Erstellt einen Datensatz auf der Grundlage der Stichprobe, indem nur noch die Spalten von Interesse enthalten sind.
    df = df[vars_of_intrest]

    # Entfernt alle Einträge, die leere Zellen enthalten.
    df = df.dropna()

    # Fügt eine neue Spalte hinzu, welche die Dummy-Variable "IS_FIN" darstellt. Für jeden Eintrag, der zu dem Sektor "Finance and Insurance" oder "Real Estate and Rental and Leasing" gehört, wird die Dummy-Variable auf "Wahr" gesetzt.
    df["IS_FIN"] = (
        (df["NAICS_SECTOR"] == "Finance and Insurance") | (df["NAICS_SECTOR"] == "Real Estate and Rental and Leasing")
    )

    # Fügt eine neue Spalte hinzu, welche den log-Wert der Spalte MARKET_CAP enthält. Die Verteilung der Daten der Variable MARKET_CAP ist rechtsschief. Ich habe gelesen, dass die Logarithmesierung die Verteilung normalisieren kann.
    df["log_MARKET_CAP"] = np.log(df["MARKET_CAP"])

    # Gleiches gilt für die Daten der abhängigen Variable PCT_EXT_TAGS. Hier habe ich versucht die Daten zu standartisieren. Dies hat natürlich auf Grund der Eigenschaften des OLS keine Auswirkungen auf das Ergebnis. Logarithmesierung ist nicht möglich, da zwei Unternehmen einen Anteil an ETEs von 0 haben.
    # df[["std_PCT_EXT_TAGS"]] = StandardScaler().fit_transform(df[["PCT_EXT_TAGS"]])

    # Erstellt auf Basis der Regressionsgleichung einen Vektor, der die unabhängige Variable enthält und eine Matrix, die das Interzept und die unabhängigen Variable enthält. 
    y, X = patsy.dmatrices("PCT_EXT_TAGS ~ log_MARKET_CAP + IS_FIN", data=df, return_type="dataframe")
    
    # Speichert den Vektor und die Matrix jeweils in einer Excel-Datei.
    y.to_excel(path_sample_regression_analyses_y_file, sheet_name="DATA")
    X.to_excel(path_sample_regression_analyses_X_file, sheet_name="DATA")

    # Erzeugt ein OLS-Objekt.
    mod = sm.OLS(y, X)

    # Führt die Schätzung der Regressionsgeraden durch.
    res = mod.fit()

    # Kontrolldiagramm: Verteilung der Variable log_MARKET_CAP
    # df.hist("log_MARKET_CAP", figsize=(8,5))
    # plt.title("Number of Companies vs log(Market Cap)")
    # plt.ylabel("Number of Companies")
    # plt.xlabel("log(Market Cap)")

    # Kontrolldiagramm: Teilregression PCT_EXT_TAGS ~ log_MARKET_CAP
    # figure = sm.graphics.plot_regress_exog(res, "log_MARKET_CAP")
    # plt.show()

    # Speichert das Ergebnis der Regression in der Variable "summary"
    summary = res.summary()

    # Speicher das Ergebnis als HTML-Datei
    with open(path_sample_regression_analyses_summary_file, "w") as file:
        file.write(summary.as_html())

    # Ausgabe des Ergebnisses auf der Konsole
    print(summary)

if __name__ == "__main__":
    main()