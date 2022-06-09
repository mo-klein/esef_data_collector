from arelle import CntlrCmdLine
from arelle import PackageManager

class CntlrItegrated(CntlrCmdLine.CntlrCmdLine):
    def __init__(self):
        super().__init__()

def main():
    cntrl = CntlrItegrated()

    testTaxonomyPackageInfo = PackageManager.packageInfo(cntrl, "C:\\Users\\Moritz Klein\\OneDrive\\05 Studium\\Master\\4 S\\Masterarbeit\\04 Stichprobe\\volkswagenag\\META-INF\\taxonomyPackage.xml")
    print(testTaxonomyPackageInfo["name"])

if __name__ == "__main__":
    main()