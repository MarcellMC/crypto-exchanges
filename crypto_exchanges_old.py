import asyncio
import websockets
import json
from datetime import datetime
import time


class WebSocketReader():

    def __init__(self):
        # Variable to keep track of timestamps and filter excessive records
        self.timestamp_tracker = 0

        # Connections pool
        self.connections = {
            'BitMEX': {
                'uri': 'wss://www.bitmex.com/realtime',
                'subscribtion': '{"op": "subscribe", "args": ["quote:XBTUSD"]}',
                'conn': None
            },
            # 'FTX': {
            #     'uri': 'wss://ftx.com/ws/',
            #     'subscribtion': '{"op": "subscribe", "channel": "orderbook", "market": "BTC-PERP"}',
            #     'conn': None
            # }
        }

    async def establish_connection(self, exchange):
        """ Opens a connection to a given WebSocket """
        uri = self.connections[exchange]['uri']
        self.connections[exchange]['conn'] = websockets.connect(uri)

    async def subscribe_and_poll(self, exchange):
        """ Connects to a given exchange and subscribes to the necessary channels """
        async with self.connections[exchange]['conn'] as websocket:
            await websocket.send(self.connections[exchange]['subscribtion'])
            async for message in websocket:
                await self.process(message)

    async def process(self, message):
        """ Processes message recieved from WebSocket """
        try:
            quotes = json.loads(message)['data']
            for quote in quotes:
                timestamp = self._convert_time(quote['timestamp'])
                if timestamp > self.timestamp_tracker:
                    self.timestamp_tracker = timestamp
                    record = ', '.join([str(timestamp), 'BitMEX', str(quote['symbol']),
                                        str(quote['bidPrice']), str(
                                            quote['bidSize']),
                                        str(quote['askPrice']), str(quote['askSize'])])
                    print(record)
        except KeyError:
            pass

    async def run_feed(self):
        """ Connects and subscribes to all exchages, gathers and outputs necessary data """
        exchanges = list(self.connections)
        feeds = []
        for exchange in exchanges:
            await self.establish_connection(exchange)
            feeds.append(self.subscribe_and_poll(exchange))
        await asyncio.gather(*feeds)

    def _convert_time(self, timestamp):
        """ Converts recieved timestamp to a number-of-seconds format """
        dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        t = time.mktime(dt.timetuple())
        return int(t)


def main():
    wsr = WebSocketReader()
    print('# timestamp, exchange, market, bid_price, bid_size, ask_price, ask_size')
    # asyncio.get_event_loop().run_until_complete(wsr.run_feed)
    asyncio.run(wsr.run_feed())


if __name__ == "__main__":
    main()
