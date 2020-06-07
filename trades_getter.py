from process_order_book_bitmex import OrderBookProcessorBitmex
import datetime as dt
import rfc3339

end = dt.datetime.utcnow()
start = dt.datetime(2020, 6, 7)

client = OrderBookProcessorBitmex(test=False)
client.get_trades(start, end)
client.get_trades(rfc3339.rfc3339(start), rfc3339.rfc3339(end))

