import json
import pathlib
import re
import datetime
import time
import urllib
from decimal import Decimal

import pytest
import responses

import backtrader as bt
import ccxtbt
from ccxtbt.exceptions import UnsupportedOrderType
from .utils.json import DecimalEncoder
from .fixtures.fake import FakeData, FakeCommInfo, BlankStrategy, FakeStrategy


@pytest.fixture(scope="class")
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

    def test_order_error_notification(self, binance_config):
        # work with testnet (order passthrough)
        broker = ccxtbt.CCXTBroker(**binance_config)
        data = FakeData("BTC/USDT")
        strategy = BlankStrategy()

        broker.start()

        broker.buy(strategy, data, exectype=bt.Order.Limit, price=1, size=0.1)
        time.sleep(1)
        assert len(broker.store.notifs) >= 1
