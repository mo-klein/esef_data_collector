from arelle import Cntlr
from arelle import PackageManager
from arelle import ModelManager
from arelle import ModelInstanceObject
from arelle.ModelInstanceObject import ModelInlineFact

import logging

class CntlrItegrated(Cntlr.Cntlr):
    def __init__(self):
        super().__init__(logFileName="logToPrint")

def main():
    cntlr = CntlrItegrated()

    url_taxonomy = "C:/Users/Moritz Klein/OneDrive/05 Studium/Master/4 S/Masterarbeit/04 Stichprobe/volkswagenag/META-INF/taxonomyPackage.xml"
    url_filing = "C:/Users/Moritz Klein/OneDrive/05 Studium/Master/4 S/Masterarbeit/04 Stichprobe/volkswagenag.xhtml"

    modelManager = ModelManager.initialize(cntlr)

    modelManager.load(url_filing, taxonomyPackages=[url_taxonomy])
    
    for fact in modelManager.modelXbrl.facts:
        print(fact.qname)


if __name__ == "__main__":
    main()