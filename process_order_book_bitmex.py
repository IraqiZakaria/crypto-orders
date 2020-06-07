import pandas as pd
import datetime as dt
import math
import bitmex
import time
import logging
import os
from six.moves import cPickle
from check_connection import check_connection


class OrderBookProcessorBitmex:

    def __init__(self, test=True):
        self.test = test
        self.client = bitmex.bitmex(test=self.test)
        self.file_base_name = os.path.dirname(os.path.abspath(__file__)) + "\data\data_orderbook_"
        self.file_base_name_trade = os.path.dirname(os.path.abspath(__file__)) + "\data\data_trades_"

    def get_trading_book(self, start_date, end_date, symbol="XBTUSD"):
        counter = 1
        full_data = []
        current_length = 1000
        try :
            while current_length > 3:
                if counter % 100 == 0:
                    file_name = self.file_base_name_trade + str(start_date).replace(":", "_").replace(".", "_")
                    full_data = self.store_data(self, file_name, full_data)

                self.client = bitmex.bitmex(test=self.test)
                added_data = []
                try:
                    added_data = \
                        self.client.Trade.Trade_get(symbol=symbol, startTime=start_date, endTime=end_date).result()[0]
                except:
                    self.client = bitmex.bitmex(test=self.test)
                    logging.warning("get_trading_book: the data could not be gotten for start_date %s and end date %s" % (
                    str(start_date), str(end_date)))
                    try:
                        time.sleep(1)
                        added_data = \
                            self.client.Trade.Trade_get(symbol=symbol, startTime=start_date, endTime=end_date).result()[0]
                    except:
                        logging.warning(
                            "get_trading_book: the data could not be gotten for start_date %s and end date %s" % (
                                str(start_date), str(end_date)))

                full_data.append(added_data)
                current_length = len(added_data)
                start_date = added_data[-1]["timestamp"]
        except KeyboardInterrupt:
            logging.warning("get_trading_book: Process interrupted by user")
            file_name = self.file_base_name_trade + str(start_date).replace(":", "_").replace(".", "_")
            full_data = self.store_data(self, file_name, full_data)
        finally:
            logging.info("get_trading_book: Process ended")


    def process_data(self, symbol="XBTUSD", sleep=1, counter=math.inf, save_location=None):
        '''

        :param sleep: number of seconds between two order data
        :param counter: number_of_seconds_to_store_in_db
        :param save_location: the data_saver_file
        :return: a dataframe containing all
        '''
        count = 1

        stored_data = []
        start_time = str(dt.datetime.utcnow()).replace(":", "_").replace(".", "_")
        file_name = self.file_base_name + start_time

        try:
            logging.info("Started generating data")

            while count < counter:
                t0 = time.time()
                count += 1
                try:
                    check_connection()
                except ConnectionError:

                    self.store_data(self, file_name, stored_data)
                    logging.warning("OrderBookProcessorBitmex::process_data, no internet connection, aborting")
                    return
                try:
                    date = dt.datetime.utcnow()
                    if count % 100 == 0:
                        logging.info("adding data for %s " % (str(date)))
                    self.client = bitmex.bitmex(test=self.test)
                    data_to_store = self.client.OrderBook.OrderBook_getL2(symbol=symbol).result()
                    stored_data.append(self.process_one_data(date, symbol, data_to_store))
                except:
                    logging.warning(
                        "OrderBookProcessorBitmex::process_data : the data was not compatible for %s" % (str(date)))
                    try:
                        # time.sleep(1)
                        self.client = bitmex.bitmex(test=self.test)
                        date = dt.datetime.utcnow()
                        data_to_store = self.client.OrderBook.OrderBook_getL2(symbol=symbol).result()
                        stored_data.append(self.process_one_data(date, symbol, data_to_store))
                    except:
                        logging.warning(
                            "OrderBookProcessorBitmex::process_data : the data was not compatible even for %s" % (
                                str(date)))

                if count % 10000 == 0:
                    logging.info("Added 10000 new data points from %s " % (start_time))
                    file_name = self.file_base_name + start_time
                    stored_data = self.store_data(self, file_name, stored_data)
                    # A new start time is being given here
                    start_time = str(dt.datetime.utcnow()).replace(":", "_").replace(".", "_")

                time.sleep(max(0, sleep - time.time() + t0))
            logging.info("Added new data points from %s " % (start_time))
            file_name = self.file_base_name + start_time
            stored_data = self.store_data(self, file_name, stored_data)
        except KeyboardInterrupt as e:
            logging.info("Recorder: Caught keyboard interrupt. \n%s" % e)
            logging.info("Added new data points from %s " % (start_time))
            file_name = self.file_base_name + start_time
            stored_data = self.store_data(self, file_name, stored_data)
            return

    @staticmethod
    def store_data(self, file_name, stored_data: list):
        stored_data = pd.DataFrame(stored_data)
        f = open(file_name, 'wb')
        cPickle.dump(stored_data, f, protocol=cPickle.HIGHEST_PROTOCOL)
        f.close()
        return []

    def process_one_data(self, date: dt.datetime, symbol: str, order_book_unprocessed: tuple):
        try:
            if order_book_unprocessed[1].reason != "OK":
                raise Exception("OrderBookProcessorBitmex::process_one_data, the status was not Ok")
        except:
            raise Exception("OrderBookProcessorBitmex::process_one_data, the status was not Ok")
        unprocessed_data = pd.DataFrame(order_book_unprocessed[0])
        if len(unprocessed_data.symbol.unique()) > 1:
            raise Exception("OrderBookProcessorBitmex::process_one_data, more than one pair")

        unprocessed_data.drop(columns="id", inplace=True)
        bids = unprocessed_data[unprocessed_data.side == "Buy"]
        asks = unprocessed_data[unprocessed_data.side == "Sell"]
        bids = bids.sort_values(by=['price'], ascending=False)
        asks = asks.sort_values(by=['price'], ascending=True)
        bids_price_cols = ["bid_price_" + str(k) for k in range(len(bids))]
        bids_size_cols = ["bid_size_" + str(k) for k in range(len(bids))]
        ask_price_cols = ["ask_price_" + str(k) for k in range(len(asks))]
        ask_size_cols = ["ask_size_" + str(k) for k in range(len(asks))]
        columns = ["timestamp", "pair"] + bids_price_cols + bids_size_cols + ask_price_cols + ask_size_cols
        values = [date, symbol] + list(bids.price) + list(bids["size"]) + list(asks.price) + list(asks["size"])
        dictionary = dict(zip(columns, values))
        return dictionary
