import filing
import company
import statistics

def main():
    filing.extract_filings()
    filing.read_filings()

    company.create_companies()
    company.save_companies()

    df = statistics.build_dataframe()

    print(df)

    

if __name__ == "__main__":
    main()