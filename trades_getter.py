from process_order_book_bitmex import OrderBookProcessorBitmex
import datetime as dt

end = dt.datetime.utcnow()
start = dt.datetime(2020, 6, 7)

client = OrderBookProcessorBitmex(test=False)
client.get_trading_book(start, end)

