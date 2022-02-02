#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2017 Ed Bartosh <bartosh@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import absolute_import, division, print_function, unicode_literals

import collections
import threading
import asyncio
import time
import json
from enum import Enum
from datetime import datetime, timezone
from functools import wraps

import backtrader as bt
import ccxt
import ccxt.async_support as ccxt_async
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass
from ccxt.base.errors import NetworkError, ExchangeError


class PollingMethod(Enum):
    OrderStatus = "order_status"
    Transaction = "transaction"


class Poller:
    def __init__(self, store, q, exchange, config={}, sandbox=False, method=None, *args, **kwargs):

        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        exch_config = config.copy()
        exch_config["asyncio_loop"] = self.loop

        self.store = store
        self.exchange = getattr(ccxt_async, exchange)(exch_config)
        self.exchange.set_sandbox_mode(sandbox)
        self.method = method
        self.q = q

        self.trades_since = {}

    async def process_updated_order(self, oref, oid):
        order = self.store.broker.orders[oref]
        symbol = order.data._dataname
        print("fetch_order")
        ccxt_order = await self.exchange.fetch_order(oid, symbol)

        if ccxt_order["status"] == "canceled":
            self.store.broker._cancel(oref)
        elif ccxt_order["status"] == "rejected":
            self.store.broker._reject(oref)
        elif ccxt_order["status"] == "expired":
            self.store.broker._expire(oref)
        self.store.open_orders.pop(oref)

    async def fetch_updated_order(self):
        while True:
            try:
                try:
                    first_oref, first_oid = next(iter(self.store.open_orders.items()))
                except StopIteration:
                    await asyncio.sleep(self.exchange.rateLimit / 1000)
                    continue
                first_order = self.store.broker.orders[first_oref]
                # 'since' in milliseconds
                first_order_date = bt.num2date(first_order.created.dt)
                first_order_timestamp = (
                    datetime.timestamp(first_order_date) * 1000
                )  # Convert to milliseconds

                symbol = first_order.data._dataname
                o_orders = await self.exchange.fetch_open_orders(
                    symbol, since=first_order_timestamp
                )
                # still open order
                o_orders_ids = [int(o["id"]) for o in o_orders]
                # order that not in open status
                changed_order = {
                    k: v for k, v in self.store.open_orders.items() if v not in o_orders_ids
                }
                tasks = [
                    asyncio.create_task(self.process_updated_order(oref, oid))
                    for oref, oid in changed_order.items()
                ]
                await asyncio.gather(*tasks)
            except Exception as e:
                print(e)
            await asyncio.sleep(self.exchange.rateLimit / 1000)

    async def fetch_transaction_per_symbol(self, symbol):
        trades = await self.exchange.fetch_my_trades(symbol, since=self.trades_since[symbol])

        if len(trades):
            since = trades[len(trades) - 1]["timestamp"]
            self.trades_since[symbol] = max(since, self.trades_since[symbol])
            for trade in trades:
                self.process_transaction(trade)
        await asyncio.sleep(1)

    def process_transaction(self, trade):
        # {
        #     'timestamp': 1642926029695,
        #     'datetime':'2022-01-23T08:20:29.695Z',
        #     'symbol':'BTC/USDT',
        #     'id':'2038231',
        #     'order':'9324938',
        #     'type':None,
        #     'side':'buy',
        #     'takerOrMaker':'taker',
        #     'price':35607.55,
        #     'amount':0.014042,
        #     'cost':500.0012171,
        #     'fee': {
        #         'cost': 0,
        #         'currency': 'BTC'
        #     },
        #     'fees': [{
        #         'cost': 0,
        #         'currency': 'BTC'
        #     }]
        # }
        oid = int(trade["order"])
        if oid in self.store._ordersrev:
            oref = self.store._ordersrev[oid]
            order = self.store.broker.orders[oref]

            if order.status in [bt.Order.Completed]:
                return

            stored_exbits = [
                {k: v for k, v in vars(x).items() if k in ["dt", "size", "price"]}
                for x in order.executed.exbits
            ]
            t = {
                "dt": datetime.utcfromtimestamp(trade["timestamp"] / 1000),
                "size": trade["amount"],
                "price": trade["price"],
            }

            if t not in stored_exbits:
                dt = t["dt"]
                size = t["size"]
                price = t["price"]

                # getposition
                psize = size
                pprice = price
                closed = 0
                opened = 0

                closedvalue = closedcomm = 0.0
                openedvalue = openedcomm = 0.0
                margin = pnl = 0.0

                # fmt: off
                order.execute(
                    dt, size, price,
                    closed, closedvalue, closedcomm,
                    opened, openedvalue, openedcomm,
                    margin, pnl,
                    psize, pprice,
                )
                # fmt: on

                self.store.broker.notify(order)
                # remove completed order from store.open_orders
                if not order.executed.remsize:
                    self.store.open_orders.pop(oref)

    async def fetch_transaction(self):
        start_since = self.exchange.milliseconds()

        while True:
            try:
                symbols = {order.data._dataname for _, order in self.store.broker.orders.items()}
                for symbol in symbols:
                    if symbol not in self.trades_since:
                        self.trades_since[symbol] = start_since
                tasks = [
                    asyncio.create_task(self.fetch_transaction_per_symbol(symbol))
                    for symbol in symbols
                ]
                await asyncio.gather(*tasks)
            except Exception as e:
                print(e)
            await asyncio.sleep(self.exchange.rateLimit / 1000)

    async def stop_async(self):
        await self.exchange.close()

    def stop(self):
        self.loop.run_until_complete(self.stop_async())

    def close(self):
        self.loop.stop()
        self.loop.close()

    def run(self):
        try:
            if self.method == PollingMethod.OrderStatus:
                poll_method = self.fetch_updated_order()
            elif self.method == PollingMethod.Transaction:
                poll_method = self.fetch_transaction()
            self.loop.run_until_complete(poll_method)
        except Exception as e:
            print(f"{e}")
        finally:
            self.stop()
            self.close()


class MetaSingleton(MetaParams):
    """Metaclass to make a metaclassed class a singleton"""

    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super(MetaSingleton, cls).__call__(*args, **kwargs)

        return cls._singleton


class CCXTStore(with_metaclass(MetaSingleton, object)):
    """API provider for CCXT feed and broker classes.

    Added a new get_wallet_balance method. This will allow manual checking of the balance.
        The method will allow setting parameters. Useful for getting margin balances

    Added new private_end_point method to allow using any private non-unified end point

    """

    params = {
        "account_poll_timeout": 5,
        "poll_timeout": 2,
        "reconnections": -1,
        "reconnect_timeout": 5,
    }

    # Supported granularities
    _GRANULARITIES = {
        (bt.TimeFrame.Minutes, 1): "1m",
        (bt.TimeFrame.Minutes, 3): "3m",
        (bt.TimeFrame.Minutes, 5): "5m",
        (bt.TimeFrame.Minutes, 15): "15m",
        (bt.TimeFrame.Minutes, 30): "30m",
        (bt.TimeFrame.Minutes, 60): "1h",
        (bt.TimeFrame.Minutes, 90): "90m",
        (bt.TimeFrame.Minutes, 120): "2h",
        (bt.TimeFrame.Minutes, 180): "3h",
        (bt.TimeFrame.Minutes, 240): "4h",
        (bt.TimeFrame.Minutes, 360): "6h",
        (bt.TimeFrame.Minutes, 480): "8h",
        (bt.TimeFrame.Minutes, 720): "12h",
        (bt.TimeFrame.Days, 1): "1d",
        (bt.TimeFrame.Days, 3): "3d",
        (bt.TimeFrame.Weeks, 1): "1w",
        (bt.TimeFrame.Weeks, 2): "2w",
        (bt.TimeFrame.Months, 1): "1M",
        (bt.TimeFrame.Months, 3): "3M",
        (bt.TimeFrame.Months, 6): "6M",
        (bt.TimeFrame.Years, 1): "1y",
    }

    BrokerCls = None  # broker class will auto register
    DataCls = None  # data class will auto register

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Returns ``DataCls`` with args, kwargs"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Returns broker with *args, **kwargs from registered ``BrokerCls``"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self, exchange, currency=None, config={}, retries=5, debug=False, sandbox=False):
        super().__init__()

        self.notifs = collections.deque()  # store notifications for cerebro

        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start

        self._env = None  # reference to cerebro for general notifications
        self._evt_acct = threading.Event()
        self._orders = collections.OrderedDict()  # map order.ref to order id
        self._ordersrev = collections.OrderedDict()  # map order id to order.ref
        self.open_orders = collections.OrderedDict()  # map open order
        self._trades = collections.OrderedDict()  # map order.ref to trade id
        self.exchange_name = exchange
        self.exchange_config = config
        self.exchange_sandbox = sandbox
        self.exchange = getattr(ccxt, exchange)(config)
        self.exchange.set_sandbox_mode(sandbox)
        self.currency = currency
        self.retries = retries
        self.debug = debug
        self.balances = dict()
        self.poller = dict()

        # balance = self.exchange.fetch_balance() if "secret" in config else 0
        # try:
        #     if balance == 0 or not balance["free"][currency]:
        #         self._cash = 0
        #     else:
        #         self._cash = balance["free"][currency]
        # except KeyError:  # never funded or eg. all USD exchanged
        #     self._cash = 0
        # try:
        #     if balance == 0 or not balance["total"][currency]:
        #         self._value = 0
        #     else:
        #         self._value = balance["total"][currency]
        # except KeyError:
        #     self._value = 0
        self._cash = 0.0  # margin available, currently available cash
        self._value = 0.0  # account balance

    def start(self, data=None, broker=None):
        # datas require some processing to kickstart data reception
        if data is None and broker is None:
            self.cash = None
            return

        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            if self.broker is not None:
                self.broker.data_started(data)

        elif broker is not None:
            self.broker = broker
            self.polling_events()
            self.broker_threads()

    def stop(self):
        # signal end of thread
        if self.broker is not None:
            self.q_account.put(None)
            self.q_ordercreate.put(None)
            self.q_orderclose.put(None)

    def put_notification(self, msg, *args, **kwargs):
        """Adds a notification"""
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Return the pending "store" notifications"""
        self.notifs.append(None)  # put a mark / threads could still append
        return [x for x in iter(self.notifs.popleft, None)]

    def get_granularity(self, timeframe, compression):
        if not self.exchange.has["fetchOHLCV"]:
            raise NotImplementedError(
                "'%s' exchange doesn't support fetching OHLCV data" % self.exchange.name
            )

        granularity = self._GRANULARITIES.get((timeframe, compression))
        if granularity is None:
            raise ValueError(
                "backtrader CCXT module doesn't support fetching OHLCV "
                "data for time frame %s, comression %s"
                % (bt.TimeFrame.getname(timeframe), compression)
            )

        if self.exchange.timeframes and granularity not in self.exchange.timeframes:
            raise ValueError(
                "'%s' exchange doesn't support fetching OHLCV data for "
                "%s time frame" % (self.exchange.name, granularity)
            )

        return granularity

    def retry(method):
        @wraps(method)
        def retry_method(self, *args, **kwargs):
            for i in range(self.retries):
                if self.debug:
                    print("{} - {} - Attempt {}".format(datetime.now(), method.__name__, i))
                time.sleep(self.exchange.rateLimit / 1000)
                try:
                    return method(self, *args, **kwargs)
                except (NetworkError, ExchangeError):
                    if i == self.retries - 1:
                        raise

        return retry_method

    @retry
    def fetch_balance(self, **kwargs):
        self.balances = self.exchange.fetch_balance(kwargs)

    def get_wallet_balance(self, currency=None, refresh=False, **kwargs):
        if refresh:
            self.fetch_balance(kwargs)
        if currency:
            return self.balances[currency]
        else:
            return self.balances

    @retry
    def get_balance(self):
        balance = self.exchange.fetch_balance()

        cash = balance["free"][self.currency]
        value = balance["total"][self.currency]
        # Fix if None is returned
        self._cash = cash if cash else 0
        self._value = value if value else 0

    @retry
    def getposition(self):
        return self._value
        # return self.getvalue(currency)

    def polling_events(self):
        q = queue.Queue()
        kwargs = {
            "q": q,
            "exchange": self.exchange_name,
            "config": self.exchange_config,
            "sandbox": self.exchange_sandbox,
        }

        t = threading.Thread(target=self._t_polling_order_status, kwargs=kwargs)
        t.daemon = True
        t.start()

        t = threading.Thread(target=self._t_polling_transaction, kwargs=kwargs)
        t.daemon = True
        t.start()

        return q

    def _t_polling_order_status(self, q, exchange, config={}, sandbox=False):
        poller = Poller(
            store=self,
            q=q,
            exchange=exchange,
            config=config,
            sandbox=sandbox,
            method=PollingMethod.OrderStatus,
        )
        self.poller["order_status"] = poller
        poller.run()

    def _t_polling_transaction(self, q, exchange, config={}, sandbox=False):
        poller = Poller(
            store=self,
            q=q,
            exchange=exchange,
            config=config,
            sandbox=sandbox,
            method=PollingMethod.Transaction,
        )
        self.poller["transaction"] = poller
        poller.run()

    def broker_threads(self):
        self.q_account = queue.Queue()
        self.q_account.put(True)  # force an immediate update
        t = threading.Thread(target=self._t_account)
        t.daemon = True
        t.start()

        self.q_ordercreate = queue.Queue()
        t = threading.Thread(target=self._t_order_create)
        t.daemon = True
        t.start()

        self.q_orderclose = queue.Queue()
        t = threading.Thread(target=self._t_order_cancel)
        t.daemon = True
        t.start()

        # Wait once for the values to be set
        self._evt_acct.wait(self.p.account_poll_timeout)

    def _t_account(self):
        while True:
            try:
                msg = self.q_account.get(timeout=self.p.account_poll_timeout)
                if msg is None:
                    break  # end of thread
            except queue.Empty:  # timeout -> time to refresh
                pass

            try:
                self.fetch_balance()
            except NetworkError as e:
                self.put_notification(f"fetch balance failed due to network error: {str(e)}")
            except ExchangeError as e:
                self.put_notification(f"fetch balance failed due to exchange error: {str(e)}")
            except Exception as e:
                self.put_notification(str(e))
                continue

            self._evt_acct.set()

    @retry
    def create_order(self, symbol, order_type, side, amount, price, params):
        # returns the order
        return self.exchange.create_order(
            symbol=symbol, type=order_type, side=side, amount=amount, price=price, params=params
        )

    def order_create(self, order, stopside=None, takeside=None, **kwargs):
        """Create an order"""
        okwargs = dict()
        params = order.info["params"] if "params" in order.info else dict(order.info)
        okwargs["symbol"] = order.data._name if order.data._name else order.data._dataname
        # TODO: check valid order type in market
        okwargs["type"] = self.broker.order_types.get(order.exectype)
        okwargs["side"] = "buy" if order.isbuy() else "sell"
        okwargs["amount"] = order.created.size
        if order.exectype in [bt.Order.Limit, bt.Order.StopLimit]:
            okwargs["price"] = order.created.price
        elif order.exectype in [bt.Order.Stop]:
            stop_price_key = self.broker.mappings["stop_price"]["key"]
            params[stop_price_key] = order.created.price
        if order.exectype == bt.Order.StopLimit:
            stop_price_key = self.broker.mappings["stop_price"]["key"]
            if order.created.plimit:
                params[stop_price_key] = order.created.plimit

        okwargs["params"] = params

        self.q_ordercreate.put(
            (
                order.ref,
                okwargs,
            )
        )

        return order

    def _t_order_create(self):
        while True:
            if self.q_ordercreate.empty():
                continue
            msg = self.q_ordercreate.get()
            if msg is None:
                continue
            oref, okwargs = msg

            try:
                ret_ord = self.exchange.create_order(**okwargs)
                # print(ret_ord)
            except Exception as e:
                self.put_notification(str(e))
                self.broker._reject(oref)
                continue

            try:
                oid = ret_ord["id"]
            except Exception as e:
                self.put_notification(f"Error from server: {str(e)}")
                self.broker._reject(oref)
                continue

            self.broker._submit(oref)
            if (
                self.broker.order_types_rev[okwargs["type"]] == bt.Order.Market
                or ret_ord["status"] == "open"
            ):
                self.broker._accept(oref)  # taken immediately

            self._orders[oref] = oid
            self._ordersrev[oid] = oref  # maps ids to backtrader order
            self.open_orders[oref] = oid

    @retry
    def cancel_order(self, order_id, symbol):
        return self.exchange.cancel_order(order_id, symbol)

    def order_cancel(self, order):
        self.q_orderclose.put(order.ref)
        return order

    def _t_order_cancel(self):
        while True:
            oref = self.q_orderclose.get()
            if oref is None:
                break
            oid = self._orders.get(oref, None)
            if oid is None:
                continue  # the order is no longer there

            try:
                order = self.orders.get(order.ref, False)
                symbol = order.data.p.dataname
                self.store.cancel_order(oid, symbol)
                self.broker._cancel(oref)
                self.store.open_orders.pop(oref)
            except Exception as e:
                self.put_notification(f"Order not cancelled: {oid}, {str(e)}")
                continue

    @retry
    def fetch_trades(self, symbol):
        return self.exchange.fetch_trades(symbol)

    @retry
    def fetch_ohlcv(self, symbol, timeframe, since, limit, params={}):
        if self.debug:
            print(
                "Fetching: {}, TF: {}, Since: {}, Limit: {}".format(symbol, timeframe, since, limit)
            )
        return self.exchange.fetch_ohlcv(
            symbol, timeframe=timeframe, since=since, limit=limit, params=params
        )

    @retry
    def fetch_order(self, oid, symbol):
        return self.exchange.fetch_order(oid, symbol)

    @retry
    def fetch_open_orders(self, symbol=None, since=None, limit=None, params={}):
        if symbol is None:
            return self.exchange.fetchOpenOrders(since=since, limit=limit, params=params)
        else:
            return self.exchange.fetchOpenOrders(
                symbol=symbol, since=since, limit=limit, params=params
            )

    @retry
    def private_end_point(self, type, endpoint, params):
        """
        Open method to allow calls to be made to any private end point.
        See here: https://github.com/ccxt/ccxt/wiki/Manual#implicit-api-methods

        - type: String, 'Get', 'Post','Put' or 'Delete'.
        - endpoint = String containing the endpoint address eg. 'order/{id}/cancel'
        - Params: Dict: An implicit method takes a dictionary of parameters, sends
          the request to the exchange and returns an exchange-specific JSON
          result from the API as is, unparsed.

        To get a list of all available methods with an exchange instance,
        including implicit methods and unified methods you can simply do the
        following:

        print(dir(ccxt.hitbtc()))
        """
        return getattr(self.exchange, endpoint)(params)
