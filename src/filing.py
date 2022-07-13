import os
import os.path
from zipfile import ZipFile
from datetime import timedelta
import hashlib

from arelle import Cntlr
from arelle import ModelManager
from arelle.ModelInstanceObject import ModelInlineFact
from arelle.ModelInstanceObject import ModelContext

import data_analytics

"""
- archives: Beinhaltet für jedes Jahr die noch nicht verarbeiteten .zip Archive der ESEF-Berichte (funktion aktuell nicht verfügbar)
- filings: Beinhaltet die entpacken ESEF-Berichte
"""

PATH_ARCHIVES = "./archives"
PATH_FILINGS = "./filings"


"""
Entpackt nacheinander alle im Ordner "archives" vorhandenen ESEF-Berichte und verschiebt die entpackten Berichte in den Ordner "filings".
Während der Arbeit mit dem Programm wurde festgestellt, dass die zip-Archive nicht immer einheitlich aufgebaut sind, daher
wird diese Methode zunächst nicht mehr unterstützt und die ESEF-Berichte müssen manuell entpackt werden.
"""

def extract_filings():

    extracted_filings = os.listdir(PATH_FILINGS)

    with os.scandir(PATH_ARCHIVES) as dir_iter:
        for entry in dir_iter:
            with ZipFile(entry.path) as zip_file:
                if zip_file.filename in extracted_filings:
                    print("WARNING: {} wurde bereits extrahiert. Bitte überprüfen!".format(zip_file.filename))

                zip_file.extractall(PATH_FILINGS)

                extracted_filings.append(zip_file.filename)

            os.remove(entry.path)

"""
Liest nacheinander alle im Ordner "filings" befindlichen ESEF-Berichte ein. Die Berichte werden nur eingelesen, sofern sie nicht schon in der Datenbank enthalten sind.
"""

def read_filings(df):
    cntlr = CntlrItegrated()

    filings = []

    with os.scandir(PATH_FILINGS) as dir_iter:
        for entry in dir_iter:
            url_filing = ""
            url_taxonomy = ""

            for root, dirs, files in os.walk(entry.path):
                for file in files:
                    if ".xhtml" in file or ".html" in file:
                        url_filing = os.path.join(root, file)

                    if "taxonomyPackage.xml" in file:
                        url_taxonomy = os.path.join(root, file)

            sha1 = ""

            if url_filing != "":
                sha1 = _calculate_filing_checksum(url_filing)

            print("\nFür den Bericht {} wurden folgende Daten gefunden:".format(entry.name))
            print("\tReport-File: {}".format(url_filing))
            print("\tReport-SHA1-Checksum: {}".format(sha1))
            print("\tTaxonomy-Package-File: {}".format(url_taxonomy))

            if not data_analytics.has_filing(df, sha1) and url_filing != "" and url_taxonomy != "":
                print("\n\t==> Bericht wird geladen.\n")

                try:
                    model_manager = ModelManager.initialize(cntlr)
                    modelXbrl = model_manager.load(url_filing, taxonomyPackages=[url_taxonomy])

                    _read_facts(filings, modelXbrl, sha1)

                    modelXbrl.close()
                except BaseException as e:
                    print("Fehler beim Laden von {}: {}".format(entry.name, e))
            else:
                print("\n\t==> Bericht wird nicht geladen (Daten schon vorhanden oder fehlerhaft).\n")

    return filings

def _calculate_filing_checksum(url_filing):
    buffer_size = 65536
    
    sha1 = hashlib.sha1()

    with open(url_filing, "rb") as file:
        while True:
            data = file.read(buffer_size)

            if not data:
                break

            sha1.update(data)

    return sha1.hexdigest()


def _read_facts(filings, modelXbrl, sha1):
    filing = ESEFFiling()

    filing.sha1 = sha1

    for fact in modelXbrl.facts:
        assert isinstance(fact, ModelInlineFact)

        esef_fact = ESEFFact()
        esef_fact.qname = "{}:{}".format(fact.qname.prefix, fact.qname.localName)
        esef_fact.value = fact.value

        if "ifrs-full" not in esef_fact.qname and "ifrs" not in esef_fact.qname:
            esef_fact.is_extension = True

        filing.facts.append(esef_fact)

        if esef_fact.qname == "ifrs-full:NameOfReportingEntityOrOtherMeansOfIdentification" or esef_fact.qname == "ifrs:NameOfReportingEntityOrOtherMeansOfIdentification":
            context = fact.context
            assert isinstance(context, ModelContext)

            scheme, identifier = context.entityIdentifier

            filing.lei = identifier

            date_period_end = (context.endDatetime - timedelta(days=1)).date()

            filing.period_end = str(date_period_end.year) + str(date_period_end.month) + str(date_period_end.day)

    if filing.lei is not None and filing.period_end is not None:
        filings.append(filing)

class CntlrItegrated(Cntlr.Cntlr):
    def __init__(self):
        super().__init__(logFileName="logToPrint")

class ESEFFiling():
    def __init__(self):
        self.lei = None
        self.period_end = None
        self.facts = []
        self.sha1 = None

class ESEFFact():
    def __init__(self):
        self.qname = None
        self.value = None
        self.is_extension = False
