import pandas as pd
import datetime as dt
import math
import bitmex
import time
import logging
import os
from six.moves import cPickle


class OrderBookProcessorBitmex:

    def __init__(self, test=True):
        self.client = bitmex.bitmex(test=test)
        self.file_base_name = os.path.dirname(os.path.abspath(__file__))

    def process_data(self, symbol="XBTUSD", sleep=1, counter=math.inf, save_location=None):
        '''

        :param sleep: number of seconds between two order data
        :param counter: number_of_seconds_to_store_in_db
        :param save_location: the data_saver_file
        :return: a dataframe containing all
        '''
        count = 1

        stored_data = []
        start_time = str(dt.datetime.today()).replace(":", "_")
        while count < counter:
            t0 = time.time()
            count += 1
            date = dt.datetime.today()
            try:
                stored_data.append(
                    self.process_one_data(date, self.client.OrderBook.OrderBook_getL2(symbol=symbol).result()))
            except:
                logging.warning(
                    "OrderBookProcessorBitmex::process_data : the data was not compatible for %s" % (str(date)))
            if count // 10000 == 0:
                stored_data = pd.DataFrame(stored_data)
                file_name = self.file_base_name + start_time
                f = open(file_name, 'wb')
                cPickle.dump(stored_data, f, protocol=cPickle.HIGHEST_PROTOCOL)
                f.close()
                stored_data = []
                start_time = str(dt.datetime.today()).replace(":", "_")

            time.sleep(max(0, sleep - time.time() + t0))

    def process_one_data(self, date: dt.datetime, order_book_unprocessed: tuple):
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
        bids.sort_values(by=['price'], inplace=True, ascending=False)
        asks.sort_values(by=['price'], inplace=True, ascending=True)
        bids_price_cols = ["bid_price_" + str(k) for k in range(len(bids))]
        bids_size_cols = ["bid_size_" + str(k) for k in range(len(bids))]
        ask_price_cols = ["ask_price_" + str(k) for k in range(len(asks))]
        ask_size_cols = ["ask_size_" + str(k) for k in range(len(asks))]
        columns = ["timestamp"] + bids_price_cols + bids_size_cols + ask_price_cols + ask_size_cols
        values = [date] + list(bids.price) + list(bids["size"]) + list(asks.price) + list(asks["size"])
        dictionary = dict(zip(columns, values))
        return dictionary
