import json
import pathlib
import re
import datetime
import time
from decimal import Decimal

import pytest
import responses
import testtools

import ccxtbt
from .utils.json import DecimalEncoder


@pytest.fixture(scope="class")
def prepare_ccxt():
    with responses.RequestsMock() as rm:
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

        yield rm


@pytest.mark.usefixtures("prepare_ccxt")
class TestBroker(testtools.TestCase):
    def test_binance_balance_spot(self):
        config = {
            "exchange": "binance",
            "config": {
                "apiKey": "tUmdJk6h6mxZ8MPQtO4qTLeo39yPrRTUJhasjxLVd9d9PO27YVkgryTJA9Scjxvh",
                "secret": "oUkFuk4TNsLtWut3PITiZlLzVl4RswKwZLsau4M48eVhKufwZfNpkD1VhraL1IWX",
            },
            "debug": True,
            "sandbox": True,
        }
        broker = ccxtbt.CCXTBroker(**config)
        broker.start()

        assert len(broker.store.balances) > 0

        prev_value = broker.store.balances["ETH"]

        delay = 0.7
        time.sleep(broker.store.p.account_poll_timeout + delay)

        last_value = broker.store.balances["ETH"]
        assert last_value["total"] > prev_value["total"]
