import asyncio
import websockets
import json
from datetime import datetime
import time


class ExchangeAbstract():

    NAME: str = 'Exchange name'
    URI: str = 'URI for WebSocket API'
    SUBSCRIBTION: str = 'Subscription command'

    data_store: dict = {}

    def __init__(self):
        # Variable to keep track of timestamps and filter excessive records
        self.timestamp_tracker = 0
        # Established connection object
        self.connection: object

    async def establish_connection(self):
        """ Opens a connection to the WebSocket endpoint """
        self.connection = websockets.connect(self.URI)

    async def subscribe_and_poll(self):
        """ Connects to the exchange and subscribes to necessary channels """
        async with self.connection as websocket:
            if (self.SUBSCRIBTION is not None):
                await websocket.send(self.SUBSCRIBTION)
            async for message in websocket:
                await self.process(message)

    async def process(self, message: str):
        """ Processes message recieved from WebSocket """
        pass

    def _mid_price_1h(self, timestamp: int):
        timestamp_1h = timestamp - 60
        if timestamp_1h in self.data_store:
            record = ExchangeAbstract.data_store[timestamp_1h]
            ask_price, bid_price = record['ask_price'], record['bid_price']
            mid_price = (ask_price + bid_price) / 2
            return mid_price
        else:
            return 'N/A'


class BitMEX(ExchangeAbstract):

    NAME = 'BitMEX'
    URI = 'wss://www.bitmex.com/realtime?subscribe=quote:XBTUSD'
    # URI = 'wss://www.bitmex.com/realtime'
    SUBSCRIBTION = None
    # SUBSCRIBTION = '{"op": "subscribe", "args": ["quote:XBTUSD"]}'

    async def process(self, message):
        """ Processes message recieved from WebSocket """
        try:
            quotes = json.loads(message)['data']
            for quote in quotes:
                timestamp = self._convert_time(quote['timestamp'])
                if timestamp > self.timestamp_tracker:
                    self.timestamp_tracker = timestamp
                    self.data_store[timestamp] = {'exchange': self.NAME, 'ticker': quote['symbol'],
                                                  'bid_price': float(quote['bidPrice']),
                                                  'bid_size': float(quote['bidSize']),
                                                  'ask_price': float(quote['askPrice']),
                                                  'ask_size': float(quote['askSize']),
                                                  'mid_price_1h': self._mid_price_1h(timestamp)}
                    output = [str(timestamp)]
                    for key, value in self.data_store[timestamp].items():
                        output.append(str(value))
                    print(output)
        except KeyError:
            pass

    def _convert_time(self, timestamp):
        """ Converts recieved timestamp to a number-of-seconds format """
        dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        t = time.mktime(dt.timetuple())
        return int(t)


class Binance(ExchangeAbstract):

    NAME = 'Binance'
    URI = 'wss://fstream.binance.com/ws/btcusdt@bookTicker'
    SUBSCRIBTION = None

    async def process(self, message):
        """ Processes message recieved from WebSocket """
        try:
            order_book = json.loads(message)
            timestamp = int(order_book['T']/1000)
            if timestamp > self.timestamp_tracker:
                self.timestamp_tracker = timestamp
                self.data_store[timestamp] = {'exchange': self.NAME, 'ticker': order_book['s'],
                                              'bid_price': float(order_book['b']),
                                              'bid_size': float(order_book['B']),
                                              'ask_price': float(order_book['a']),
                                              'ask_size': float(order_book['A']),
                                              'mid_price_1h': self._mid_price_1h(timestamp)}
                output = [str(timestamp)]
                for key, value in self.data_store[timestamp].items():
                    output.append(str(value))
                print(output)
        except KeyError:
            pass

    def _convert_time(self, timestamp):
        """ Converts recieved timestamp to a number-of-seconds format """
        dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        t = time.mktime(dt.timetuple())
        return int(t)


class WebSocketReader():

    def __init__(self, exchanges: list):
        # List of 'Exchange' instances (child classes of ExchangeAbstract)
        self.exchanges = exchanges

    async def run_feed(self):
        """ Connects and subscribes to all exchages, gathers and outputs necessary data """
        feeds = []
        for exchange in self.exchanges:
            await exchange.establish_connection()
            feeds.append(exchange.subscribe_and_poll())
        await asyncio.gather(*feeds)


def main():
    wsr = WebSocketReader([BitMEX(), Binance()])
    print('# timestamp, exchange, market, bid_price, bid_size, ask_price, ask_size')
    asyncio.run(wsr.run_feed())


if __name__ == "__main__":
    main()
