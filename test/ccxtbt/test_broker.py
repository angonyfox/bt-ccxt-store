import json
import pathlib
import re
import datetime
import time
import urllib
from decimal import Decimal

import pytest
import responses
from aioresponses import aioresponses, CallbackResult

import backtrader as bt
import ccxtbt
from ccxtbt.exceptions import UnsupportedOrderType
from .utils.json import DecimalEncoder
from .fixtures.fake import FakeData, FakeCommInfo, BlankStrategy, FakeStrategy


@pytest.fixture()
def prepare_ccxt():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rm:
        fixtures_path = pathlib.Path(__file__).parent

        # exchange info
        with fixtures_path.joinpath("fixtures/binance_exchange_info.json").open() as f:
            request_return = json.load(f)
            pattern = re.compile(r"^https://.*/api/v3/exchangeInfo")
            rm.add(responses.GET, pattern, json=request_return)

        def balance_callback(request):
            now = datetime.datetime.today()
            updatetime = int(now.timestamp() * 1000)
            value = Decimal(now.strftime("%-S"))
            headers = {}
            resp_body = {
                "makerCommission": "0",
                "takerCommission": "0",
                "buyerCommission": "0",
                "sellerCommission": "0",
                "canTrade": True,
                "canWithdraw": False,
                "canDeposit": False,
                "updateTime": updatetime,
                "accountType": "SPOT",
                "balances": [
                    {"asset": "BNB", "free": "1000.00000000", "locked": "0.00000000"},
                    {"asset": "BTC", "free": "1", "locked": "0.00000000"},
                    {"asset": "BUSD", "free": "10000.00000000", "locked": "0.00000000"},
                    {"asset": "ETH", "free": value, "locked": "0.00000000"},
                    {"asset": "LTC", "free": "500.00000000", "locked": "0.00000000"},
                    {"asset": "TRX", "free": "500000.00000000", "locked": "0.00000000"},
                    {"asset": "USDT", "free": "10000.00000000", "locked": "0.00000000"},
                    {"asset": "XRP", "free": "50000.00000000", "locked": "0.00000000"},
                ],
                "permissions": ["SPOT"],
            }

            return (200, headers, json.dumps(resp_body, cls=DecimalEncoder))

        # balance
        pattern = re.compile(r"^https://.*/api/v\d/account\?.*")
        rm.add_callback(responses.GET, pattern, callback=balance_callback)

        # order
        pattern = re.compile(r"^https://.*/api/v3/order")
        order_id = iter(range(1000, 2000))

        def order_callback(request):
            # payload = json.loads(request.body)
            payload = urllib.parse.parse_qsl(request.body.decode("utf-8"))
            payload = dict(payload)
            quantity = payload["quantity"]
            if payload["type"].upper() == "MARKET":
                # price = "0.00000000"
                executed_qty = quantity
                cum_quote_qty = "4205.73086379"
                status = "FILLED"
            else:
                executed_qty = "0.00000000"
                cum_quote_qty = "0.00000000"
                status = "NEW"
            if "price" in payload:
                price = payload["price"]
            else:
                price = "0.00000000"

            headers = {}
            resp_body = {
                "symbol": payload["symbol"],
                "orderId": next(order_id),
                "orderListId": "-1",
                "clientOrderId": payload["newClientOrderId"],
                "transactTime": payload["timestamp"],
                "price": price,
                "origQty": quantity,
                "executedQty": executed_qty,
                "cummulativeQuoteQty": cum_quote_qty,
                "status": status,
                "timeInForce": "GTC",
                "type": payload["type"],
                "side": payload["side"],
            }
            if payload["type"].upper() in ["STOP_LOSS", "STOP_LOSS_LIMIT"]:
                resp_body["stopPrice"] = payload["stopPrice"]
            return (200, headers, json.dumps(resp_body))

        rm.add_callback(responses.POST, pattern, callback=order_callback)

        yield rm


@pytest.mark.usefixtures("prepare_ccxt")
class TestBroker:
    @pytest.fixture(scope="class")
    def binance_config(self):
        config = {
            "exchange": "binance",
            "config": {
                "apiKey": "tUmdJk6h6mxZ8MPQtO4qTLeo39yPrRTUJhasjxLVd9d9PO27YVkgryTJA9Scjxvh",
                "secret": "oUkFuk4TNsLtWut3PITiZlLzVl4RswKwZLsau4M48eVhKufwZfNpkD1VhraL1IWX",
            },
            "debug": True,
            "sandbox": True,
            "broker_mapping": {
                "order_types": {
                    bt.Order.Market: "MARKET",
                    bt.Order.Limit: "LIMIT",
                    bt.Order.Stop: "STOP_LOSS",
                    bt.Order.StopLimit: "STOP_LOSS_LIMIT",
                }
            },
        }
        return config

    def test_binance_balance_spot(self, binance_config):
        broker = ccxtbt.CCXTBroker(**binance_config)
        broker.start()

        assert len(broker.store.balances) > 0

        prev_value = broker.store.balances["ETH"]

        delay = 0.7
        time.sleep(broker.store.p.account_poll_timeout + delay)

        last_value = broker.store.balances["ETH"]
        assert last_value["total"] > prev_value["total"]

    def test_order_transmit(self, binance_config):
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData()
        strategy = BlankStrategy()
        broker.start()
        broker.buy(strategy, data, size=1)
        assert len(broker.orders) == 1

        broker.sell(strategy, data, size=1)
        assert len(broker.orders) == 2

    def test_order_not_transmit(self, binance_config):
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData()
        strategy = BlankStrategy()
        broker.start()

        broker.buy(strategy, data, size=1, transmit=False)
        assert len(broker.opending) == 1

        broker.sell(strategy, data, size=1, transmit=False)
        assert len(broker.opending) == 2

    def test_order_unsupport_order_type(self, binance_config):
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData()
        strategy = BlankStrategy()

        broker.start()
        with pytest.raises(UnsupportedOrderType) as e_info:
            broker.buy(strategy, data, exectype=bt.Order.StopTrailLimit, size=1)

    def test_order(self, binance_config):
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData("BTC/USDT")
        strategy = BlankStrategy()

        broker.start()

        try:
            # Market Order
            order = broker.buy(strategy, data, size=0.1)
            time.sleep(0.2)
            assert order.ref in broker.store._orders

            # # Market order - price will be IGNORED
            broker.buy(strategy, data, size=0.05, price=1.01)
            msg = broker.store.q_ordercreate.get()
            assert "price" not in msg[1]

            # Limit Order
            broker.buy(strategy, data, exectype=bt.Order.Limit, price=40000, size=0.1)
            time.sleep(0.2)
            assert len(broker.store.notifs) >= 0

            # Stop Order
            stop_price = 50000
            broker.buy(strategy, data, exectype=bt.Order.Stop, price=stop_price, size=0.1)
            msg = broker.store.q_ordercreate.get()
            stop_price_key = broker.mappings["stop_price"]["key"]
            assert "params" in msg[1]
            assert stop_price_key in msg[1]["params"]
            assert msg[1]["params"][stop_price_key] == stop_price

            # StopLimit Order
            price = 45000
            broker.buy(
                strategy,
                data,
                exectype=bt.Order.StopLimit,
                price=price,
                plimit=stop_price,
                size=0.1,
            )
            msg = broker.store.q_ordercreate.get()
            stop_price_key = broker.mappings["stop_price"]["key"]
            assert "price" in msg[1]
            assert msg[1]["price"] == price
            assert "params" in msg[1]
            assert stop_price_key in msg[1]["params"]
            assert msg[1]["params"][stop_price_key] == stop_price

        except Exception as e:
            pytest.fail(f"Unexpected Error: {str(e)}")

    def test_order_params(self, binance_config):
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData("BTC/USDT")
        strategy = BlankStrategy()

        broker.start()
        try:
            price = 45000
            stop_price = 50000

            # StopLimit Order
            broker.buy(
                strategy,
                data,
                exectype=bt.Order.StopLimit,
                price=price,
                stopPrice=stop_price,
                size=0.1,
            )
            msg = broker.store.q_ordercreate.get()
            stop_price_key = broker.mappings["stop_price"]["key"]
            assert "price" in msg[1]
            assert msg[1]["price"] == price
            assert "params" in msg[1]
            assert stop_price_key in msg[1]["params"]
            assert msg[1]["params"][stop_price_key] == stop_price

            broker.buy(
                strategy,
                data,
                exectype=bt.Order.StopLimit,
                price=price,
                params={"stopPrice": stop_price},
                size=0.1,
            )
            msg = broker.store.q_ordercreate.get()
            stop_price_key = broker.mappings["stop_price"]["key"]
            assert "price" in msg[1]
            assert msg[1]["price"] == price
            assert "params" in msg[1]
            assert stop_price_key in msg[1]["params"]
            assert msg[1]["params"][stop_price_key] == stop_price

        except Exception as e:
            pytest.fail(f"Unexpected Error: {str(e)}")

    def test_order_brackets(self, binance_config):
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData()
        strategy = BlankStrategy()
        broker.start()

        try:
            p1 = 1
            p2 = p1 - 0.02
            p3 = p1 + 0.02
            o1 = broker.buy(
                strategy, data, exectype=bt.Order.Limit, price=p1, size=1, transmit=False
            )
            o2 = broker.sell(
                strategy, data, exectype=bt.Order.Stop, price=p2, size=1, parent=o1, transmit=False
            )
            o3 = broker.sell(
                strategy, data, exectype=bt.Order.Limit, price=p3, size=1, parent=o1, transmit=True
            )
            assert len(broker.brackets) == 1
            assert len(broker.brackets[1]) == 3
        except Exception as e:
            pytest.fail(f"Unexpected Error: {str(e)}")

    def test_order_submit(self, binance_config):
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData("BTC/USDT")
        strategy = BlankStrategy()

        broker.start()
        try:
            order = broker.buy(strategy, data, exectype=bt.Order.Limit, price=40000, size=0.1)
            assert order.status == bt.Order.Created
            time.sleep(0.2)
            assert order.status == bt.Order.Submitted
        except Exception as e:
            pytest.fail(f"Unexpected Error: {str(e)}")

    def test_order_reject(self, binance_config):
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData("BTC/USDT")
        strategy = BlankStrategy()

        broker.start()
        try:
            # Generate exception and reject
            order = broker.buy(strategy, data, size=0.1, reduceOnly=True)
            time.sleep(0.2)
            assert order.status == bt.Order.Rejected
        except Exception as e:
            pytest.fail(f"Unexpected Error: {str(e)}")

    def test_market_order_accept(self, binance_config):
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData("BTC/USDT")
        strategy = BlankStrategy()

        broker.start()
        try:
            order = broker.buy(strategy, data, size=0.1)
            time.sleep(0.2)
            assert order.status == bt.Order.Accepted
        except Exception as e:
            pytest.fail(f"Unexpected Error: {str(e)}")

    def test_open_status_order_accept(self, binance_config):
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData("BTC/USDT")
        strategy = BlankStrategy()

        broker.start()
        try:
            order = broker.buy(strategy, data, exectype=bt.Order.Limit, price=40000, size=0.1)
            time.sleep(0.2)
            assert order.status == bt.Order.Accepted
        except Exception as e:
            pytest.fail(f"Unexpected Error: {str(e)}")

    def test_order_error_notification(self, binance_config):
        # work with testnet (order passthrough)
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData("BTC/USDT")
        strategy = BlankStrategy()

        broker.start()

        # price not in PERCENT_PRICE filters range
        broker.buy(strategy, data, exectype=bt.Order.Limit, price=1, size=0.1)
        time.sleep(1)
        assert len(broker.store.notifs) >= 1

    def test_transaction_since(self, binance_config):
        fixtures_path = pathlib.Path(__file__).parent
        with aioresponses() as resp_mock:
            with fixtures_path.joinpath("fixtures/binance_exchange_info.json").open() as f:
                request_return = json.load(f)
                pattern = re.compile(r"^https://.*/api/v3/exchangeInfo")
                resp_mock.get(pattern, payload=request_return, repeat=True)

            future_time = int(time.time() * 1000)
            expected_time = {
                "BTCUSDT": future_time + 1000,
                "ETHUSDT": future_time + 2000,
                "BNBUSDT": future_time + 3000,
            }

            def my_trades_callback(url, **kwargs):
                symbol = url.query["symbol"]
                timestamp = expected_time[symbol]

                payload = [
                    {
                        "symbol": symbol,
                        "id": 205286,
                        "orderId": 954246,
                        "orderListId": -1,
                        "price": "80000.00000000",
                        "qty": "0.00200000",
                        "quoteQty": "160.00000000",
                        "commission": "0.00000000",
                        "commissionAsset": "USDT",
                        "time": 1642926029695,
                        "isBuyer": False,
                        "isMaker": True,
                        "isBestMatch": True,
                    },
                    {
                        "symbol": url.query["symbol"],
                        "id": 2038231,
                        "orderId": 9324938,
                        "orderListId": -1,
                        "price": "35607.55000000",
                        "qty": "0.01404200",
                        "quoteQty": "500.00121710",
                        "commission": "0.00000000",
                        "commissionAsset": "BTC",
                        "time": timestamp,
                        "isBuyer": True,
                        "isMaker": False,
                        "isBestMatch": True,
                    },
                ]
                return CallbackResult(payload=payload)

            mytrades_pattern = re.compile(r"^https://.*/api/v3/myTrades")
            resp_mock.get(mytrades_pattern, callback=my_trades_callback, repeat=True)

            broker = ccxtbt.CCXTBroker(**binance_config)
            strategy = BlankStrategy()
            broker.start()

            # create 3 orders with different symbol
            for idx, symbol in enumerate(["BTC/USDT", "ETH/USDT", "BNB/USDT"]):
                data = FakeData(symbol)
                order = bt.BuyOrder(owner=strategy, data=data, size=0.4, exectype=bt.Order.Limit)
                order.created.dt = datetime.datetime(2022, 1, 23, 15, 10, 0).toordinal()
                oid = 9324931 + idx
                # store order in broker and store
                broker.orders[order.ref] = order
                broker.store._orders[order.ref] = oid
                broker.store._ordersrev[oid] = order.ref
                broker.store.open_orders[order.ref] = oid

            time.sleep(3)
            assert (
                broker.store.poller["transaction"].trades_since["BTC/USDT"]
                == expected_time["BTCUSDT"]
            )
            assert (
                broker.store.poller["transaction"].trades_since["ETH/USDT"]
                == expected_time["ETHUSDT"]
            )
            assert (
                broker.store.poller["transaction"].trades_since["BNB/USDT"]
                == expected_time["BNBUSDT"]
            )

    def test_order_events_partial(self, binance_config):
        fixtures_path = pathlib.Path(__file__).parent
        with aioresponses() as resp_mock:
            with fixtures_path.joinpath("fixtures/binance_exchange_info.json").open() as f:
                request_return = json.load(f)
                pattern = re.compile(r"^https://.*/api/v3/exchangeInfo")
                resp_mock.get(pattern, payload=request_return)

            with fixtures_path.joinpath("fixtures/binance_open_orders.json").open() as f:
                open_order_pattern = re.compile(r"^https://.*/api/v3/openOrders")
                request_return = json.load(f)
                resp_mock.get(open_order_pattern, payload=request_return)

            # partial fill
            filled_size = 5
            with fixtures_path.joinpath("fixtures/binance_my_trades.json").open() as f:
                mytrades_pattern = re.compile(r"^https://.*/api/v3/myTrades")
                request_return = json.load(f)
                request_return = request_return[: filled_size + 1]
                resp_mock.get(mytrades_pattern, payload=request_return)

            broker = ccxtbt.CCXTBroker(**binance_config)
            data = FakeData("BTC/USDT")
            strategy = BlankStrategy()
            broker.start()

            # Patch trades_since to specific time to make fetch_my_trades return mocked trade
            broker.store.poller["transaction"].trades_since["BTC/USDT"] = 1642926029000

            # create partial fill order
            order = bt.BuyOrder(owner=strategy, data=data, size=0.4, exectype=bt.Order.Limit)
            order.created.dt = datetime.datetime(2022, 1, 23, 15, 10, 0).toordinal()
            oid = 9324938
            # store order in broker and store
            broker.orders[order.ref] = order
            broker.store._orders[order.ref] = oid
            broker.store._ordersrev[oid] = order.ref
            broker.store.open_orders[order.ref] = oid

            time.sleep(3)

            executed_order = broker.orders[order.ref]
            # order status change to partial
            assert executed_order.status == bt.Order.Partial
            # store trade in exbits (not insert duplicate trade)
            assert len(executed_order.executed.exbits) == filled_size
            # fire notification and only store trade of open orders
            assert len(broker.notifs.queue) == filled_size
            # remove completed order from store.open_orders
            assert order.ref in broker.store.open_orders

    def test_order_events_completed(self, binance_config):
        fixtures_path = pathlib.Path(__file__).parent
        with aioresponses() as resp_mock:
            with fixtures_path.joinpath("fixtures/binance_exchange_info.json").open() as f:
                request_return = json.load(f)
                pattern = re.compile(r"^https://.*/api/v3/exchangeInfo")
                resp_mock.get(pattern, payload=request_return)

            with fixtures_path.joinpath("fixtures/binance_open_orders.json").open() as f:
                open_order_pattern = re.compile(r"^https://.*/api/v3/openOrders")
                request_return = json.load(f)
                resp_mock.get(open_order_pattern, payload=request_return)

            # completed fill
            with fixtures_path.joinpath("fixtures/binance_my_trades.json").open() as f:
                mytrades_pattern = re.compile(r"^https://.*/api/v3/myTrades")
                request_return = json.load(f)
                resp_mock.get(mytrades_pattern, payload=request_return)

            with fixtures_path.joinpath("fixtures/binance_fetch_order_completed.json").open() as f:
                fetch_order_pattern = re.compile(r"^https://.*/api/v3/order")
                request_return = json.load(f)
                resp_mock.get(fetch_order_pattern, payload=request_return)

            broker = ccxtbt.CCXTBroker(**binance_config)
            data = FakeData("BTC/USDT")
            strategy = BlankStrategy()
            broker.start()

            # Patch trades_since to specific time to make fetch_my_trades return mocked trade
            broker.store.poller["transaction"].trades_since["BTC/USDT"] = 1642926029000

            # create partial fill order (multiple fill)
            c1_order = bt.BuyOrder(owner=strategy, data=data, size=0.4, exectype=bt.Order.Limit)
            c1_order.created.dt = datetime.datetime(2022, 1, 23, 15, 10, 0).toordinal()
            oid = 9324938
            # store order in broker and store
            broker.orders[c1_order.ref] = c1_order
            broker.store._orders[c1_order.ref] = oid
            broker.store._ordersrev[oid] = c1_order.ref
            broker.store.open_orders[c1_order.ref] = oid

            # create completed fill order (single fill)
            c2_order = bt.BuyOrder(
                owner=strategy, data=data, price=80000, size=0.002, exectype=bt.Order.Limit
            )
            c2_order.created.dt = datetime.datetime(2022, 1, 23, 15, 10, 0).toordinal()
            oid = 954246
            # store order in broker and store
            broker.orders[c2_order.ref] = c2_order
            broker.store._orders[c2_order.ref] = oid
            broker.store._ordersrev[oid] = c2_order.ref
            broker.store.open_orders[c2_order.ref] = oid

            time.sleep(3)

            c1_executed_order = broker.orders[c1_order.ref]
            # order status change to partial
            assert c1_executed_order.status == bt.Order.Partial
            # store trade in exbits (not insert duplicate trade)
            assert len(c1_executed_order.executed.exbits) == 21
            # still keep order from store.open_orders
            assert c1_order.ref in broker.store.open_orders

            c2_executed_order = broker.orders[c2_order.ref]
            # order status change to partial
            assert c2_executed_order.status == bt.Order.Completed
            # store trade in exbits (not insert duplicate trade)
            assert len(c2_executed_order.executed.exbits) == 1
            # remove completed order from store.open_orders
            assert c2_order.ref not in broker.store.open_orders

            # fire notification
            assert len(broker.notifs.queue) == 22

    def test_order_events_canceled(self, binance_config):
        fixtures_path = pathlib.Path(__file__).parent
        with aioresponses() as resp_mock:
            with fixtures_path.joinpath("fixtures/binance_exchange_info.json").open() as f:
                request_return = json.load(f)
                pattern = re.compile(r"^https://.*/api/v3/exchangeInfo")
                resp_mock.get(pattern, payload=request_return)

            with fixtures_path.joinpath("fixtures/binance_open_orders.json").open() as f:
                open_order_pattern = re.compile(r"^https://.*/api/v3/openOrders")
                request_return = json.load(f)
                resp_mock.get(open_order_pattern, payload=request_return)

            mytrades_pattern = re.compile(r"^https://.*/api/v3/myTrades")
            request_return = {}
            resp_mock.get(mytrades_pattern, payload=request_return)

            with fixtures_path.joinpath("fixtures/binance_fetch_order_canceled.json").open() as f:
                fetch_order_pattern = re.compile(r"^https://.*/api/v3/order")
                request_return = json.load(f)
                resp_mock.get(fetch_order_pattern, payload=request_return)

            broker = ccxtbt.CCXTBroker(**binance_config)
            data = FakeData("BTC/USDT")
            strategy = BlankStrategy()
            broker.start()

            # create canceled order
            order = bt.BuyOrder(owner=strategy, data=data, size=0.4, exectype=bt.Order.Limit)
            order.created.dt = datetime.datetime(2022, 1, 23, 15, 10, 0).toordinal()
            oid = 9324939
            # store order in broker and store
            broker.orders[order.ref] = order
            broker.store._orders[order.ref] = oid
            broker.store._ordersrev[oid] = order.ref
            broker.store.open_orders[order.ref] = oid

            time.sleep(2)

            # order status change to canceled
            assert broker.orders[order.ref].status == bt.Order.Canceled
            # notify order
            assert order in broker.notifs.queue
            # remove from store.open_orders
            assert order.ref not in broker.store.open_orders

    def test_order_events_rejected(self, binance_config):
        fixtures_path = pathlib.Path(__file__).parent
        with aioresponses() as resp_mock:
            with fixtures_path.joinpath("fixtures/binance_exchange_info.json").open() as f:
                request_return = json.load(f)
                pattern = re.compile(r"^https://.*/api/v3/exchangeInfo")
                resp_mock.get(pattern, payload=request_return)

            with fixtures_path.joinpath("fixtures/binance_open_orders.json").open() as f:
                open_order_pattern = re.compile(r"^https://.*/api/v3/openOrders")
                request_return = json.load(f)
                resp_mock.get(open_order_pattern, payload=request_return)

            mytrades_pattern = re.compile(r"^https://.*/api/v3/myTrades")
            request_return = {}
            resp_mock.get(mytrades_pattern, payload=request_return)

            with fixtures_path.joinpath("fixtures/binance_fetch_order_rejected.json").open() as f:
                fetch_order_pattern = re.compile(r"^https://.*/api/v3/order")
                request_return = json.load(f)
                resp_mock.get(fetch_order_pattern, payload=request_return)

            broker = ccxtbt.CCXTBroker(**binance_config)
            data = FakeData("BTC/USDT")
            strategy = BlankStrategy()
            broker.start()

            # create rejected order
            order = bt.BuyOrder(owner=strategy, data=data, size=0.4, exectype=bt.Order.Limit)
            order.created.dt = datetime.datetime(2022, 1, 23, 15, 10, 0).toordinal()
            oid = 9324940
            # store order in broker and store
            broker.orders[order.ref] = order
            broker.store._orders[order.ref] = oid
            broker.store._ordersrev[oid] = order.ref
            broker.store.open_orders[order.ref] = oid

            time.sleep(2)

            # order status change to rejected
            assert broker.orders[order.ref].status == bt.Order.Rejected
            # notify order
            assert order in broker.notifs.queue
            # remove from store.open_orders
            assert order.ref not in broker.store.open_orders

    def test_order_events_expired(self, binance_config):
        fixtures_path = pathlib.Path(__file__).parent
        with aioresponses() as resp_mock:
            with fixtures_path.joinpath("fixtures/binance_exchange_info.json").open() as f:
                request_return = json.load(f)
                pattern = re.compile(r"^https://.*/api/v3/exchangeInfo")
                resp_mock.get(pattern, payload=request_return)

            with fixtures_path.joinpath("fixtures/binance_open_orders.json").open() as f:
                open_order_pattern = re.compile(r"^https://.*/api/v3/openOrders")
                request_return = json.load(f)
                resp_mock.get(open_order_pattern, payload=request_return)

            mytrades_pattern = re.compile(r"^https://.*/api/v3/myTrades")
            request_return = {}
            resp_mock.get(mytrades_pattern, payload=request_return)

            with fixtures_path.joinpath("fixtures/binance_fetch_order_expired.json").open() as f:
                fetch_order_pattern = re.compile(r"^https://.*/api/v3/order")
                request_return = json.load(f)
                resp_mock.get(fetch_order_pattern, payload=request_return)

            broker = ccxtbt.CCXTBroker(**binance_config)
            data = FakeData("BTC/USDT")
            strategy = BlankStrategy()
            broker.start()

            # create expired order
            order = bt.BuyOrder(
                owner=strategy,
                data=data,
                size=0.4,
                exectype=bt.Order.Limit,
                valid=datetime.timedelta(-1),
            )
            order.created.dt = datetime.datetime(2022, 1, 23, 15, 10, 0).toordinal()
            oid = 9324941
            # store order in broker and store
            broker.orders[order.ref] = order
            broker.store._orders[order.ref] = oid
            broker.store._ordersrev[oid] = order.ref
            broker.store.open_orders[order.ref] = oid

            time.sleep(2)

            # order status change to rejected
            assert broker.orders[order.ref].status == bt.Order.Expired
            # notify order
            assert order in broker.notifs.queue
            # remove from store.open_orders
            assert order.ref not in broker.store.open_orders
