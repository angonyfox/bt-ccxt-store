import backtrader as bt
import datetime


class FakeCommInfo(object):
    def getvaluesize(self, size, price):
        return 0

    def profitandloss(self, size, price, newprice):
        return 0

    def getoperationcost(self, size, price):
        return 0.0

    def getcommission(self, size, price):
        return 0.0


class FakeData(object):
    """
    Minimal interface to avoid errors when trade tries to get information from
    the data during the test
    """

    def __init__(self, name="FakeData"):
        self._name = name
        self._dataname = name
        self._datetime = bt.LineBuffer()
        self._close = bt.LineBuffer()

        self._datetime.forward()
        self._close.forward()

        self._datetime[0] = bt.date2num(datetime.datetime.today())
        self._close[0] = 1

        # parameter
        self.p = bt.utils.DotDict({"sessionend": datetime.datetime.today()})

    def __len__(self):
        return 0

    @property
    def datetime(self):
        # return [0.0]
        return self._datetime

    @property
    def close(self):
        # return [0.0]
        return self._close

    def date2num(self, dt):
        return bt.date2num(dt)


class BlankStrategy(object):
    pass


class FakeStrategy(bt.Strategy):
    def __init__(self):
        self.next_runs = 0

    def next(self, dt=None):
        dt = dt or self.datas[0].datetime.datetime(0)
        print("%s closing price: %s" % (dt.isoformat(), self.datas[0].close[0]))
        self.next_runs += 1
