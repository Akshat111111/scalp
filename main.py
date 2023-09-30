import alpaca_trade_api as alpaca
import asyncio
import pandas as pd
import sys
import logging

logger = logging.getLogger()

class ScalpAlgo:

    def __init__(self, api, symbol, lot):
        self._api = api
        self._symbol = symbol
        self._lot = lot
        self._bars = []
        self._l = logger.getChild(self._symbol)
        self._state = None

        now = pd.Timestamp.now(tz='America/New_York').floor('1min')
        market_open = now.replace(hour=9, minute=30)
        today = now.strftime('%Y-%m-%d')
        tomorrow = (now + pd.Timedelta('1day')).strftime('%Y-%m-%d')
        data = api.polygon.historic_agg_v2(
            symbol, 1, 'minute', today, tomorrow, unadjusted=False).df
        bars = data[market_open:]
        self._bars = bars
        self._order = None
        self._position = None

    def _outofmarket(self):
        return pd.Timestamp.now(tz='America/New_York').time() >= pd.Timestamp('15:55').time()

    def checkup(self, position):
        now = pd.Timestamp.now(tz='America/New_York')
        order = self._order

        if order and order.side == 'buy' and (now - order.submitted_at > pd.Timedelta('2 min')):
            last_trade = self._api.polygon.last_trade(self._symbol)
            self._l.info(
                f'canceling missed buy order {order.id} at {order.limit_price} (current price = {last_trade.price})')
            self._cancel_order()

        if self._position and self._outofmarket():
            self._submit_sell(bailout=True)

    def _cancel_order(self):
        if self._order:
            self._api.cancel_order(self._order.id)

    def _calc_buy_signal(self):
        mavg = self._bars.rolling(20).mean().close.values
        closes = self._bars.close.values
        return closes[-2] < mavg[-2] and closes[-1] > mavg[-1]

    def on_bar(self, bar):
        self._bars = self._bars.append(pd.DataFrame({
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
        }, index=[bar.start]))

        if len(self._bars) < 21 or self._outofmarket() or self._state == 'SELL_SUBMITTED':
            return

        if self._state == 'TO_BUY':
            signal = self._calc_buy_signal()
            if signal:
                self._submit_buy()

    def on_order_update(self, event, order):
        self._l.info(f'order update: {event} = {order}')
        if event == 'fill':
            self._order = None
            if self._state == 'BUY_SUBMITTED':
                self._position = self._api.get_position(self._symbol)
                self._transition('TO_SELL')
                self._submit_sell()
            elif self._state == 'SELL_SUBMITTED':
                self._position = None
                self._transition('TO_BUY')
        elif event == 'partial_fill':
            self._position = self._api.get_position(self._symbol)
            self._order = self._api.get_order(order['id'])
        elif event in ('canceled', 'rejected'):
            if event == 'rejected':
                self._l.warn(f'order rejected: current order = {self._order}')
            self._order = None
            if self._state == 'BUY_SUBMITTED':
                if self._position:
                    self._transition('TO_SELL')
                    self._submit_sell()
                else:
                    self._transition('TO_BUY')
            elif self._state == 'SELL_SUBMITTED':
                self._transition('TO_SELL')
                self._submit_sell(bailout=True)

    def _submit_buy(self):
        trade = self._api.polygon.last_trade(self._symbol)
        amount = int(self._lot / trade.price)
        try:
            order = self._api.submit_order(
                symbol=self._symbol,
                side='buy',
                type='limit',
                qty=amount,
                time_in_force='day',
                limit_price=trade.price,
            )
        except Exception as e:
            self._l.info(e)
            self._transition('TO_BUY')
            return

        self._order = order
        self._l.info(f'submitted buy {order}')
        self._transition('BUY_SUBMITTED')

    def _submit_sell(self, bailout=False):
        params = {
            'symbol': self._symbol,
            'side': 'sell',
            'qty': self._position.qty if self._position else 0,
            'time_in_force': 'day',
        }
        if bailout:
            params['type'] = 'market'
        else:
            current_price = float(self._api.polygon.last_trade(self._symbol).price)
            cost_basis = float(self._position.avg_entry_price)
            limit_price = max(cost_basis + 0.01, current_price)
            params.update({
                'type': 'limit',
                'limit_price': limit_price,
            })
        try:
            order = self._api.submit_order(**params)
        except Exception as e:
            self._l.error(e)
            self._transition('TO_SELL')
            return

        self._order = order
        self._l.info(f'submitted sell {order}')
        self._transition('SELL_SUBMITTED')

    def _transition(self, new_state):
        self._l.info(f'transition from {self._state} to {new_state}')
        self._state = new_state

def main(args):
    api = alpaca.REST()
    stream = alpaca.StreamConn()
    fleet = {}
    symbols = args.symbols

    for symbol in symbols:
        algo = ScalpAlgo(api, symbol, lot=args.lot)
        fleet[symbol] = algo

    @stream.on(r'^AM')
    async def on_bars(conn, channel, data):
        if data.symbol in fleet:
            fleet[data.symbol].on_bar(data)

    @stream.on(r'trade_updates')
    async def on_trade_updates(conn, channel, data):
        logger.info(f'trade_updates {data}')
        symbol = data.order['symbol']
        if symbol in fleet:
            fleet[symbol].on_order_update(data.event, data.order)

    async def periodic():
        while True:
            if not api.get_clock().is_open:
                logger.info('Exit as the market is not open')
                sys.exit(0)
            await asyncio.sleep(30)
            positions = api.list_positions()
            for symbol, algo in fleet.items():
                pos = [p for p in positions if p.symbol == symbol]
                algo.checkup(pos[0] if len(pos) > 0 else None)

    channels = ['trade_updates'] + [
        'AM.' + symbol for symbol in symbols
    ]

    loop = stream.loop
    loop.run_until_complete(asyncio.gather(
        stream.subscribe(channels),
        periodic(),
    ))
    loop.close()

if __name__ == '__main__':
    import argparse

    fmt = '%(asctime)s:%(filename)s:%(lineno)d:%(levelname)s:%(name)s:%(message)s'
    logging.basicConfig(level=logging.INFO, format=fmt)
    fh = logging.FileHandler('console.log')
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter(fmt))
    logger.addHandler(fh)

    parser = argparse.ArgumentParser()
    parser.add_argument('symbols', nargs='+')
    parser.add_argument('--lot', type=float, default=2000)

    main(parser.parse_args())


