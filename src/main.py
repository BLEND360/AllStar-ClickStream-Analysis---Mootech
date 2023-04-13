# noinspection PyTypeChecker
from data_ingestion import DataIngest


def main():
    """
    initializes the DataIngest Class and retrieves the data since last load
    for transaction tables and the latest for SCD tables
    """
    # update transactions tables
    data_ingest = DataIngest(scope='mootech-scope', key='mootech-key')
    start_date = datetime.date(2023, 4, 5)

    data_ingest.get_data_by_range(table='clickstream', start_date=start_date)

    data_ingest.get_data_by_range(table='transactions', start_date=start_date)
    # update SCD tables
    day_before_yesterday = datetime.datetime.utcnow() - timedelta(days=2)
    yesterday = datetime.datetime.utcnow() - timedelta(days=1)

    data_ingest.get_data_by_range(table='users',
                                  start_date=day_before_yesterday,
                                  end_date=yesterday,
                                  )

    data_ingest.get_data_by_range(table='products',
                                  start_date=day_before_yesterday,
                                  end_date=yesterday,
                                  )
    data_ingest.run_fetch()


if __name__ == "__main__":
    main()
