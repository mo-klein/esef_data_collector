import os
from zipfile import ZipFile

from arelle import Cntlr
from arelle import ModelManager
from arelle import FileSource
from arelle.ModelInstanceObject import ModelInlineFact

from company import Company

class CntlrItegrated(Cntlr.Cntlr):
    def __init__(self):
        super().__init__(logFileName="logToPrint")

def main():

    vw = Company("529900NNUPAGGOMPXZ31")
    vw.get_company_data()

    print(vw.trbc_economic_sector)
    print(vw.company_market_cap["2020"])

    #_unzip_filings()
    #_read_filings()

_path_to_archives = "./archives"
_path_to_filings = "./filings"

def _unzip_filings():
    print("1. Schritt - Entpacken der ESEF-Berichte".center(24, "#"))

    n = len(os.listdir(_path_to_archives))
    i = 1

    for item in os.scandir(_path_to_archives):
        with ZipFile(item.path) as zip:
            
            print("Entpacke {} - {}/{} ({}%)".format(item.name, i, n, i/n * 100))

            zip.extractall(_path_to_filings)

            i += 1

        os.remove(item.path)

# Taxonomiepakete befinden sich stets in der Datei META-INF/taxonomyPackage.xml
# Berichte befinden sich stets in der Datei reports/bericht.xhtml
def _read_filings():
    cntlr = CntlrItegrated()

    print("2. Schritt - Lesen der ESEF-Berichte".center(24, "#"))

    n = len(os.listdir(_path_to_filings))
    i = 1

    for item in os.scandir(_path_to_filings):
        url_filing = ""
        url_taxonomy = ""

        for r, d, f in os.walk(item.path):
            for file in f:
                if(".xhtml") in file:
                    url_filing = os.path.join(r, file)

                if "taxonomyPackage.xml" in file:
                    url_taxonomy = os.path.join(r, file)

        if url_filing != "" and url_taxonomy != "":
            print("Lese {} - {}/{} ({}%)".format(item.name, i, n, i/n * 100))

            try:
                model_manager = ModelManager.initialize(cntlr)
                modelXbrl = model_manager.load(url_filing, taxonomyPackages=[url_taxonomy])

                data = _extract_data(modelXbrl)
            except:
                print("Fehler beim Laden - {}".format(item.name))

            i += 1

def _extract_data(modelXbrl):
    data = []

    for fact in modelXbrl.facts:
        data.append(
            [str(fact.qname),
            str(fact.value)]
        )

    return data   

if __name__ == "__main__":
    main()