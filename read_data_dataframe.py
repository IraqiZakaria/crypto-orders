from six.moves import cPickle

filename = "F:\Trading\crypto-orders\data\data_orderbook_2020-06-07 10_47_52_621560"
with open(filename, "rb") as fh:
    allprof = cPickle.load(fh)

b = 0
