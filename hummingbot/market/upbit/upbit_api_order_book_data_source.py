#!/usr/bin/env python

import aiohttp
import asyncio
import json
import logging
import pandas as pd
import time
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
)
import websockets
from websockets.exceptions import ConnectionClosed
from hummingbot.market.upbit import upbit_constants

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.utils import async_ttl_cache
from hummingbot.logger import HummingbotLogger
from hummingbot.market.upbit.upbit_order_book import UpbitOrderBook

NaN = float("nan")


class UpbitAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _upbitobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._upbitobds_logger is None:
            cls._upbitobds_logger = logging.getLogger(__name__)
        return cls._upbitobds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None):
        super().__init__()
        self._trading_pairs: Optional[List[str]] = trading_pairs

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have trading pair as index and include usd volume, baseAsset and quoteAsset
        """
        async with aiohttp.ClientSession() as client:

            market_response = await client.get(upbit_constants.UPBIT_MARKET_URL)
            if market_response.status != 200:
                raise IOError(f"Error fetching Upbit markets information. "
                              f"HTTP status is {market_response.status}.")
            market_response: aiohttp.ClientResponse = market_response

            market_data = await market_response.json()
            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=market_data, index="market")
            markets: List[str] = list(all_markets.index)
            currencies = [markets[i].split("-") for i in range(len(markets))]
            base_currency = [currencies[i][1] for i in range(len(currencies))]
            quote_currency = [currencies[i][0] for i in range(len(currencies))]

            high_price: List[float] = []
            low_price: List[float] = []
            quotePrice: List[float] = []
            prev_closing_price: List[float] = []
            baseVolume: List[float] = []
            quoteVolume: List[float] = []

            params: Dict[str, str] = {"markets": ','.join(markets)}
            async with client.get(upbit_constants.UPBIT_TICKER_URL, params=params) as ticker_response:
                ticker_response: aiohttp.ClientResponse = ticker_response
                if ticker_response.status == 200:
                    tickerData: List[int, any] = await ticker_response.json()
                    for i in range(len(tickerData)):
                        high_price.append(float(tickerData[i].get("high_price", NaN)))
                        low_price.append(float(tickerData[i].get("low_price", NaN)))
                        quotePrice.append(float(tickerData[i].get("trade_price", NaN)))
                        prev_closing_price.append(float(tickerData[i].get("prev_closing_price", NaN)))
                        baseVolume.append(float(tickerData[i].get("acc_trade_volume_24h", NaN)))
                        quoteVolume.append(float(tickerData[i].get("acc_trade_price_24h", NaN)))

                elif ticker_response.status != 200:
                    raise IOError(f"Error fetching Upbit ticker information."
                                  f"HTTP status is {ticker_response.status}.")
                await asyncio.sleep(0.5)
            all_markets["baseAsset"] = base_currency
            all_markets["quoteAsset"] = quote_currency
            all_markets["high"] = high_price
            all_markets["low"] = low_price
            all_markets["quotePrice"] = quotePrice
            all_markets["close"] = prev_closing_price
            all_markets["baseVolume"] = baseVolume
            all_markets["quoteVolume"] = quoteVolume

            btc_usdt_price: float = all_markets.loc["USDT-BTC"].quotePrice
            idr_usdt_price: float = all_markets.loc["IDR-USDT"].quotePrice
            usd_volume: List[float] = []
            for row in all_markets.itertuples():
                product_name: str = row.Index
                quote_volume: float = row.quoteVolume
                if product_name.startswith("USDT"):
                    usd_volume.append(quote_volume)
                elif product_name.startswith("BTC"):
                    usd_volume.append(quote_volume * btc_usdt_price)
                elif product_name.startswith("IDR"):
                    usd_volume.append(quote_volume * idr_usdt_price)
                else:
                    usd_volume.append(NaN)
                    cls.logger().error(f"Unable to convert volume to USD for market {product_name}")
            all_markets["USDVolume"] = usd_volume
            return all_markets.sort_values("USDVolume", ascending=False)

    async def get_trading_pairs(self) -> List[str]:
        if not self._trading_pairs:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._trading_pairs = active_markets.index.tolist()
            except Exception:
                self._trading_pairs = []
                self.logger().network(
                    f"Error getting active ticker information.",
                    exc_info=True,
                    app_warning_msg=f"Error getting active ticker information. Check network connection."
                )
        return self._trading_pairs

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        # Takes the bids and asks from 15 orders
        params: Dict = {"markets": trading_pair}
        async with client.get(upbit_constants.UPBIT_ORDERBOOK_URL, params=params) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching Upbit market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            api_data = await response.read()
            data: Dict[str, Any] = json.loads(api_data)[0]
            asks = [[] for _ in range(len(data["orderbook_units"]))]
            bids = [[] for _ in range(len(data["orderbook_units"]))]
            for i in range(len(data["orderbook_units"])):
                asks[i].extend(
                    [(data["orderbook_units"][i]["ask_price"]),
                     (data["orderbook_units"][i]["ask_size"])]
                )
                bids[i].extend(
                    [(data["orderbook_units"][i]["bid_price"]),
                     (data["orderbook_units"][i]["bid_size"])]
                )
            data.update(
                {"asks": asks,
                 "bids": bids}
            )
            del data["orderbook_units"]
            return data

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, OrderBookTrackerEntry] = {}

            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                    snapshot_msg: OrderBookMessage = UpbitOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        metadata={"trading_pair": trading_pair}
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    retval[trading_pair] = OrderBookTrackerEntry(
                        trading_pair,
                        snapshot_msg.timestamp,
                        order_book
                    )
                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index + 1}/{number_of_pairs} completed.")
                    await asyncio.sleep(0.4)
                except Exception:
                    self.logger().error(f"Error getting snapshot for {trading_pair}. ", exc_info=True)
                    await asyncio.sleep(5)
            return retval

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()

                async with websockets.connect(upbit_constants.UPBIT_WS_URI, compression='deflate') as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    request: List[Any] = [{"ticket": "test"},
                                          {"type": "trade",
                                           "codes": trading_pairs,
                                           "isOnlyRealtime": True}]
                    await ws.send(json.dumps(request))

                    async for raw_msg in self._inner_messages(ws):
                        msg: Dict[str, Any] = json.loads(raw_msg.decode('utf-8'))
                        trading_pair = msg["code"]
                        trade_message: OrderBookMessage = UpbitOrderBook.trade_message_from_exchange(
                            msg,
                            metadata={"trading_pair": trading_pair}
                        )
                        output.put_nowait(trade_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with websockets.connect(upbit_constants.UPBIT_WS_URI) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    request: List[Any] = [
                        {"ticket": "test"},
                        {"type": "orderbook",
                         "codes": trading_pairs,
                         "isOnlyRealtime": True}
                    ]
                    await ws.send(json.dumps(request))

                    async for raw_msg in self._inner_messages(ws):
                        msg: Dict[str, Any] = json.loads(raw_msg.decode('utf-8'))
                        units_length: int = len(msg["orderbook_units"])
                        asks_ws = [[] for _ in range(units_length)]
                        bids_ws = [[] for _ in range(units_length)]
                        for i in range(units_length):
                            asks_ws[i].extend(
                                [(msg["orderbook_units"][i]["ask_price"]),
                                 (msg["orderbook_units"][i]["ask_size"])]
                            )
                            bids_ws[i].extend(
                                [(msg["orderbook_units"][i]["bid_price"]),
                                 (msg["orderbook_units"][i]["bid_size"])]
                            )
                        msg.update(
                            {"asks": asks_ws,
                             "bids": bids_ws}
                        )
                        del msg["orderbook_units"]
                        order_book_message: OrderBookMessage = UpbitOrderBook.diff_message_from_exchange(msg)
                        output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with aiohttp.ClientSession() as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_message: OrderBookMessage = UpbitOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(snapshot_message)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
                            await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
