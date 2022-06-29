import os
import os.path
from re import A
from zipfile import ZipFile
import copy
from datetime import date, timedelta, datetime

import pprint

from arelle import ModelXbrl
from arelle import Cntlr
from arelle import ModelManager
from arelle import FileSource
from arelle.ModelInstanceObject import ModelInlineFact
from arelle.ModelInstanceObject import ModelContext
from arelle.ModelDtsObject import ModelConcept
from arelle.ModelDtsObject import ModelRoleType

"""
Definiere drei Ordner mit folgenden Funktionen:
- archives: Beinhaltet für jedes Jahr die noch nicht verarbeiteten .zip Archive der ESEF-Berichte
- filings: Beinhaltet die entpacken ESEF-Berichte
- not-valid: Beinhaltet alle nicht einlesbaren ESEF-Berichte
"""

PATH_ARCHIVES = "./archives"
PATH_FILINGS = "./filings"
PATH_NOT_VALID = "./not_valid"

FILINGS = []

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

# Taxonomiepakete befinden sich stets in der Datei META-INF/taxonomyPackage.xml
# Berichte befinden sich stets in der Datei reports/bericht.xhtml
def read_filings():
    cntlr = CntlrItegrated()

    with os.scandir(PATH_FILINGS) as dir_iter:
        for entry in dir_iter:
            url_filing = ""
            url_taxonomy = ""

            for root, dirs, files in os.walk(entry.path):
                for file in files:
                    if(".xhtml") in file:
                        url_filing = os.path.join(root, file)

                    if "taxonomyPackage.xml" in file:
                        url_taxonomy = os.path.join(root, file)

            if url_filing != "" and url_taxonomy != "":
                try:
                    model_manager = ModelManager.initialize(cntlr)
                    modelXbrl = model_manager.load(url_filing, taxonomyPackages=[url_taxonomy])

                    read_facts(modelXbrl)

                    modelXbrl.close()
                except:
                    print("Fehler beim Laden von {}".format(entry.name))

def read_facts(modelXbrl):
    filing = ESEFFiling()

    for fact in modelXbrl.facts:
        assert isinstance(fact, ModelInlineFact)

        esef_fact = ESEFFact()
        esef_fact.qname = "{}:{}".format(fact.qname.prefix, fact.qname.localName)
        esef_fact.value = fact.value

        if "ifrs-full" not in esef_fact.qname:
            esef_fact.is_extension = True

        filing.facts.append(esef_fact)

        if esef_fact.qname == "ifrs-full:NameOfReportingEntityOrOtherMeansOfIdentification":
            context = fact.context
            assert isinstance(context, ModelContext)

            scheme, identifier = context.entityIdentifier

            filing.lei = identifier

            date_period_end = (context.endDatetime - timedelta(days=1)).date()

            filing.period_end = str(date_period_end.year) + str(date_period_end.month) + str(date_period_end.day)

    if filing.lei != "" and filing.period_end != "":
        FILINGS.append(filing)

class CntlrItegrated(Cntlr.Cntlr):
    def __init__(self):
        super().__init__(logFileName="logToPrint")

class ESEFFiling():
    def __init__(self):
        self.lei = None
        self.period_end = None
        self.facts = []

class ESEFFact():
    def __init__(self):
        self.qname = None
        self.value = None
        self.is_extension = False
