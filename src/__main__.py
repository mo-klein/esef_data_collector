import filing
import company
import data_analytics
import eikon_hook

def main():
    eikon_hook.set_app_key()

    df = data_analytics.load_dataframe()

    filings = filing.read_filings(df)

    companies = company.create_companies(filings)
    company.save_companies(companies)

    df = data_analytics.append_companies(df, companies)

    data_analytics.group_by_country(df)
    data_analytics.group_by_sector(df)

    data_analytics.show_graph_all(df)

if __name__ == "__main__":
    main()