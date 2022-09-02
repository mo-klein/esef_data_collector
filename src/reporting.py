import json
import logging
import os
import os.path
from datetime import timedelta
import hashlib
import shutil
import time
from typing import Any

from arelle import Cntlr
from arelle import ModelManager
from arelle.ModelInstanceObject import ModelInlineFact
from arelle.ModelInstanceObject import ModelContext
from arelle.ModelXbrl import ModelXbrl

import numpy as np
import pandas as pd

PATH_IMPORT_DIR = "./import"

def load_reports(sha1_checksums_of_existing_reports: pd.Series, path_sample_esef_packages_dir: str, path_sample_reports_dir: str) -> list:
    start_time = time.time()
    
    # Deaktiviert den Logger von Arelle für eine "saubere" Konsolenausgabe.
    logging.getLogger("arelle").setLevel(100)

    cntlr = CntlrItegrated()

    reports = []

    # Dict zum abspeichern von nicht einlesbaren Berichten und des korrespondierenden Fehlers
    not_loadable_esef_packages = {}

    # Schrittweises Durchlaufen aller Elemente im import-Ordner
    with os.scandir(PATH_IMPORT_DIR) as dir_iter:
        for esef_package in dir_iter:

            if esef_package.is_file():
                print("\nEs wurde folgende Datei im import-Ordner gefunden: {}\nBitte beachten Sie die Anforderungen zum Import an die ESEF-Pakete.".format(esef_package.name))
                continue

            print("\nESEF-Paket \"{}\" wird geladen:".format(esef_package.name))

            url_report_file = ""
            url_taxonomy_package_file = ""

            # Durchläuft das aktuelle Verzeichnis und sucht (auch in Sub-Verzeichnissen) nach der Berichts- und der Taxonomiedatei.
            for root, dirs, files in os.walk(esef_package.path):
                for file in files:
                    if ".xhtml" in file or ".html" in file:
                        url_report_file = os.path.join(root, file).replace("\\", "/")

                    if "taxonomyPackage.xml" in file:
                        url_taxonomy_package_file = os.path.join(root, file).replace("\\", "/")

            # Meldung eines Fehler, falls für das aktuelle Paket keine Berichts- oder Taxonomiedatei gefunden wurde.
            if url_report_file == "" or url_taxonomy_package_file == "":
                err_msg = "Berichts- oder Taxonomiedatei nicht vorhanden. Bericht wird nicht geladen."
                print("\n\t==> {}".format(err_msg))
                not_loadable_esef_packages[esef_package.name] = err_msg

                continue
            
            # Berechnung der SHA1-Prüfsumme der Berichtsdatei
            report_sha1_checksum = _calculate_report_checksum(url_report_file)

            # Ausgabe von Informationen über das Berichtspaket auf der Konsole
            print("\tReport-File: {}".format(url_report_file))
            print("\tReport-SHA1-Checksum: {}".format(report_sha1_checksum))
            print("\tTaxonomy-Package-File: {}".format(url_taxonomy_package_file))

            # Überprüfung, ob der aktuelle Bericht bereits im Sample enthalten ist.
            if report_sha1_checksum in sha1_checksums_of_existing_reports.values:
                print("\n\t==> Bericht schon vorhanden. Bericht wird nicht geladen.")

                continue

            print("\n\t==> XBRL-Elemente (Tags) werden nun gelesen.")

            try:
                model_manager = ModelManager.initialize(cntlr)
                modelXbrl = model_manager.load(url_report_file, taxonomyPackages=[url_taxonomy_package_file])

                report = _read_tags(modelXbrl, esef_package.name, path_sample_reports_dir)

                # Prüft, ob der Bericht gelesen werden konnte.
                if len(report) == 0:
                    print("\n\tDas ESEF-Paket \"{}\" ist unvollständig und konnte nicht gelesen werden!".format(esef_package.name))

                    not_loadable_esef_packages[esef_package.name] = "Unvollständige Daten zur Indentifizierung des Unternehmens!"

                    continue

                report.append(report_sha1_checksum)
                reports.append(report)

                shutil.move(esef_package.path, path_sample_esef_packages_dir)

                print("\n\tESEF-Paket \"{}\" wurde erfolgreich geladen.".format(esef_package.name))

                modelXbrl.close()
            except BaseException as e:
                print("\t\tBeim Lesen der XBRL-Elemente (Tags) des ESEF-Paktes \"{}\" ist ein Fehler in der Arelle-Plattform aufgetreten.".format(esef_package.name))
                not_loadable_esef_packages[esef_package.name] = e

    if not_loadable_esef_packages:
        print("\nFolgende ESEF-Pakete konnten aufgrund eines Fehlers nicht geladen werden:")

        for esef_package_name, e in not_loadable_esef_packages.items():
            print("\n\t{}:".format(esef_package_name))
            print("\t{}".format(e))

    end_time = time.time()

    print("\nBearbeitungsdauer: {} (HH:MM:SS)".format(timedelta(seconds=(end_time-start_time))))

    return reports

def _calculate_report_checksum(url_filing: str) -> str:
    buffer_size = 65536
    
    sha1 = hashlib.sha1()

    with open(url_filing, "rb") as file:
        while True:
            data = file.read(buffer_size)

            if not data:
                break

            sha1.update(data)

    return sha1.hexdigest()

def _read_tags(modelXbrl: ModelXbrl, esef_package_name: str, path_sample_reports_dir: str) -> list:
    # Liste als Speicher für alle Tags im Bericht. Tags werden als Tuple in der Liste abgelegt.
    tags = []

    # Erfassung der wichtigsten Eigenschaften des Berichts
    lei = ""
    period_end = ""

    # Zählvariablen zum Zählen der Anzahl an Elementen der Basistaxonomie und an Elementen der Erweiterungstaxonomie. Berechnung einer Anzahl auch im Umkehrschluss aus der anderen anhand der Gesamtanzahl möglich.
    # Zur besseren Verständnis des Codes wurde hier darauf verzichtet.
    count_esef_tags = 0
    count_ext_tags = 0

    # Ein Fakt ist für das Verständnis an dieser Stelle vereinfachend gleichzusetzen mit dem Begriff Tag. Tatsächlich handelt es sich um ein Wert, der mit einer rechnungslegungsbezogenen Bedeutung (Taxonomie) und einem Kontext verknüpft ist.
    # "By combining a concept (profit) from a taxonomy (say Canadian GAAP) with a value (1000) and the needed context (Acme Corporation, for the period 1 January 2015 to 31 January 2015 in Canadian Dollars) we arrive at a fact.", Getting Started for Developers, XBRL International
    
    for fact in modelXbrl.facts:
        assert isinstance(fact, ModelInlineFact)

        # Erfassung der wichtigsten Eigenschaften des Facts
        #  qname: Qualifizierter Name des Facts/Tags (Z.b. ifrs-full:Revenue)
        #  value: Der Wert/Inhalt des Tags
        #  is_extension: Erfasst die Tatsache, ob es sich bei dem Fact um eine Element der Erweiterungstaxonomie des Unternehmens handelt.
        qname = "{}:{}".format(fact.qname.prefix, fact.qname.localName)
        value = fact.value
        is_extension = False

        if "ifrs-full" not in qname and "ifrs" not in qname:
            is_extension = True
            count_ext_tags += 1
        else:
            count_esef_tags += 1

        tags.append((qname, value, is_extension))
        
        # Prüfen, ob es sich bei dem aktuellen Tag, um das Elemente ifrs-full:NameOfReportingEntityOrOtherMeansOfIdentification der Basistaxonomie handelt.
        # Definition des Elements: "Name des berichterstattenden Unternehmens oder andere Mittel der Identifizierung" (EU-VO 2018/815 S. 602)
        # Diesem Element lässt sich auch der Legal Entity Identifier (LEI) und das Periodenende entnehmen.

        if qname.split(":")[1] == "NameOfReportingEntityOrOtherMeansOfIdentification":
            context = fact.context
            assert isinstance(context, ModelContext)

            scheme, lei = context.entityIdentifier

            date_period_end = (context.endDatetime - timedelta(days=1)).date()
            period_end = str(date_period_end.year) + "{:02d}".format(date_period_end.month) + str(date_period_end.day)

    # Abschließend wird überprüft, ob alle Eigenschaften des Unternehmens ausgelesen werden konnten. Ist diese Bedingung erfüllt wird der Bericht zu einem Datensatz zusammengefasst und zurückgegeben.
    if lei and period_end:
        count_all_tags = len(tags)
        pct_all_tags = round((float(count_esef_tags) + float(count_ext_tags))/float(count_all_tags) * 100, 2) # Kontrollvariable
        pct_esef_tags = round(float(count_esef_tags) / float(count_all_tags) * 100, 2)
        pct_ext_tags = round(float(count_ext_tags) / float(count_all_tags) * 100, 2)

        _save_report(esef_package_name, tags, path_sample_reports_dir)

        return [esef_package_name, lei, period_end, count_all_tags, pct_all_tags, count_esef_tags, pct_esef_tags, count_ext_tags, pct_ext_tags]
    else:
        return []

def _serialize(obj) -> dict[str, Any]:
    return vars(obj)

def _save_report(esef_package_name: str, tags: list, path_sample_reports_dir: str):
    path_sample_reports_report_file = "{}/{}.json".format(path_sample_reports_dir, esef_package_name)
    
    print("\n\tSpeichern der Tags unter \"{}\".".format(path_sample_reports_report_file))

    with open(path_sample_reports_report_file, "w") as file:
        json.dump(tags, file, default=_serialize, indent=4)

# Integrierter Arelle Controller, der Informationen und Hinweise auf dem Standard Ausgabe Stream (Konsole) ausgibt.
class CntlrItegrated(Cntlr.Cntlr):
    def __init__(self):
        super().__init__(logFileName="logToPrint")
