import bitmex
import pandas as pd
client = bitmex.bitmex()
client = bitmex.bitmex(test=False)
result = client.OrderBook.OrderBook_getL2(symbol="XBTUSD").result()
a = 0