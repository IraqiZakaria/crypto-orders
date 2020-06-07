from process_order_book_bitmex import OrderBookProcessorBitmex

client = OrderBookProcessorBitmex(test=False)
client.process_data(sleep=1)
