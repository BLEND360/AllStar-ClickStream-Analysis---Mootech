# noinspection PyTypeChecker
from data_ingestion import DataIngest
from src.Transformations.month_over_month import MoMTransformer
from src.Transformations.setup_sliver_layer import SilverLayer
from src.utils.common_functions import get_latest_transaction_date, save_to_gold_layer
import datetime


def ingest():
    """
    initializes the DataIngest Class and retrieves the data since last load
    for transaction tables and the latest for SCD tables
    """
    # update transactions tables
    data_ingest = DataIngest(scope='mootech-scope', key='mootech-key')
    start_date = get_latest_transaction_date()

    data_ingest.get_data_by_range(table='clickstream', start_date=start_date)

    data_ingest.get_data_by_range(table='transactions', start_date=start_date)
    # update SCD tables
    day_before_yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=2)
    yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)

    data_ingest.get_data_by_range(table='users',
                                  start_date=day_before_yesterday,
                                  end_date=yesterday,
                                  )

    data_ingest.get_data_by_range(table='products',
                                  start_date=day_before_yesterday,
                                  end_date=yesterday,
                                  )
    data_ingest.run_fetch()


def main():
    """
    Ingests data and saves to bronze layer
    Sets up Silver layer with transactions table
    Calculates month-over-month sales report for item
    :return: None
    """
    # The item we want the sales report for
    item_to_be_queried = 'tumbler'

    # Ingest new data
    ingest()

    # setup and save data into silver layer
    silver_layer = SilverLayer()
    silver_layer.setup_transactions()
    silver_layer.save_transactions()

    # get transformer
    transformer = MoMTransformer()

    # get month_over_month_report
    sales_report = transformer.transform(item_to_be_queried)
    print('report generated successfully')

    # save report to gold layer
    save_to_gold_layer(sales_report, f"sales_report_{item_to_be_queried}")
    print('report saved to gold layer')


if __name__ == "__main__":
    main()
