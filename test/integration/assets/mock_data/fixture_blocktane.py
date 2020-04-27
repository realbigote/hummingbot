class FixtureBlocktane:
    PING = "200"

    BALANCES = [{'currency': 'usd', 'balance': '102049651.77676', 'locked': '49987.0'}, {'currency': 'btc', 'balance': '0.0', 'locked': '0.0'}, {'currency': 'fth', 'balance': '201998999.33074', 'locked': '999.76'}, {'currency': 'eth', 'balance': '0.0', 'locked': '0.0'}, {'currency': 'trst', 'balance': '0.0', 'locked': '0.0'}]

    MARKETS = [{"id": "fthusd", "name": "FTH/USD", "base_unit": "fth", "quote_unit": "usd", "min_price": "0.01", "max_price": "1000.0", "min_amount": "0.01", "amount_precision": 2, "price_precision": 2, "state": "enabled"}, {"id": "ethusd", "name": "ETH/USD", "base_unit": "eth", "quote_unit": "usd", "min_price": "0.01", "max_price": "1000.0", "min_amount": "0.00001", "amount_precision": 5, "price_precision": 2, "state": "enabled"}]

    MARKETS_TICKERS = {"fthusd":{"at":1587567587,"ticker":{"buy":"50.0","sell":"51.0","low":"50.0","high":"51.0","open":50.0,"last":"50.0","volume":"2660.5","avg_price":"50.500560045104303702311595565","price_change_percent":"+0.00%","vol":"2660.5"}},"ethusd":{"at":1587567587,"ticker":{"buy":"0.0","sell":"0.0","low":"0.0","high":"0.0","open":"0.0","last":"0.0","volume":"0.0","avg_price":"0.0","price_change_percent":"+0.00%","vol":"0.0"}}}

    ORDER_MARKET_OPEN_BUY = {"id":10001,"side":"buy","ord_type":"market","price":None,"avg_price":"0.0","state":"pending","market":"fthusd","created_at":"2020-04-22T17:02:52+02:00","updated_at":"2020-04-22T17:02:52+02:00","origin_volume":"0.02","remaining_volume":"0.02","executed_volume":"0.0","trades_count":0}

    ORDER_MARKET_OPEN_SELL = {"id":10000,"side":"sell","ord_type":"market","price":None,"avg_price":"0.0","state":"pending","market":"fthusd","created_at":"2020-04-22T17:02:52+02:00","updated_at":"2020-04-22T17:02:52+02:00","origin_volume":"0.02","remaining_volume":"0.02","executed_volume":"0.0","trades_count":0}

    WS_ORDER_MARKET_BUY_FILLED = {"order":{"id":10001,"market":"fthusd","kind":"bid","side":"buy","ord_type":"market","price":None,"avg_price":"51.0","state":"done","origin_volume":"0.02","remaining_volume":"0.0","executed_volume":"0.02","at":1587674368,"created_at":1587674368,"updated_at":1587674368,"trades_count":1}}
    WS_ORDER_MARKET_SELL_FILLED = {"order":{"id":10000,"market":"fthusd","kind":"bid","side":"buy","ord_type":"market","price":None,"avg_price":"51.0","state":"done","origin_volume":"0.02","remaining_volume":"0.0","executed_volume":"0.02","at":1587674368,"created_at":1587674368,"updated_at":1587674368,"trades_count":1}}

    ORDER_PLACE_OPEN = {"id": 10001, "side": "buy", "ord_type": "limit", "price": "50.0", "avg_price": "0.0", "state": "pending", "market": "fthusd", "created_at": "2020-04-23T18:01:49+02:00", "updated_at": "2020-04-23T18:01:49+02:00", "origin_volume": "0.02", "remaining_volume": "0.02", "executed_volume": "0.0", "trades_count": 0}

    ORDER_CANCEL = {"id": 10001, "side": "buy", "ord_type": "limit", "price": "50.0", "avg_price": "0.0", "state": "wait", "market": "fthusd", "created_at": "2020-04-23T18:01:49+02:00", "updated_at": "2020-04-23T18:01:50+02:00", "origin_volume": "0.02", "remaining_volume": "0.02", "executed_volume": "0.0", "trades_count": 0}
    ORDER_CANCEL_1 = {"id": 10001, "side": "buy", "ord_type": "limit", "price": "50.0", "avg_price": "0.0", "state": "wait", "market": "fthusd", "created_at": "2020-04-23T18:01:49+02:00", "updated_at": "2020-04-23T18:01:50+02:00", "origin_volume": "0.02", "remaining_volume": "0.02", "executed_volume": "0.0", "trades_count": 0}

    ORDERS_OPEN_BUY = '[{"id": 10001, "side": "sell", "ord_type": "limit", "price": "51.0", "avg_price": "0.0", "state": "wait", "market": "fthusd", "created_at": "2020-04-22T16:14:06+02:00", "updated_at": "2020-04-22T16:14:06+02:00", "origin_volume": "0.02", "remaining_volume": "0.02", "executed_volume": "0.0", "trades_count": 0}]'
    ORDERS_OPEN_SELL = [{"id": 10000, "side": "buy", "ord_type": "limit", "price": "50.0", "avg_price": "0.0", "state": "wait", "market": "fthusd", "created_at": "2020-04-22T16:13:48+02:00", "updated_at": "2020-04-22T16:13:48+02:00", "origin_volume": "0.02", "remaining_volume": "0.02", "executed_volume": "0.0", "trades_count": 0}]

    MARKETS_DEPTH = {'timestamp': 1587652652, 'asks': [['51.0', '994.76']], 'bids': [['50.0', '999.7'], ['40.8', '0.04']]}

    WS_ORDER_FILLED_SELL_LIMIT = {"order":{"id":10000,"market":"fthusd","kind":"bid","side":"buy","ord_type":"limit","price":"50.5","avg_price":"50.5","state":"done","origin_volume":"0.02","remaining_volume":"0.0","executed_volume":"0.02","at":1587673109,"created_at":1587673109,"updated_at":1587673144,"trades_count":1}}

    WS_ORDER_FILLED_BUY_LIMIT = {"order":{"id":10001,"market":"fthusd","kind":"ask","side":"sell","ord_type":"limit","price":"50.5","avg_price":"50.5","state":"done","origin_volume":"0.02","remaining_volume":"0.0","executed_volume":"0.02","at":1587673109,"created_at":1587673109,"updated_at":1587673144,"trades_count":1}}

    WS_ORDER_CANCEL_BUY_LIMIT = {"order":{"id":10001,"market":"fthusd","kind":"bid","side":"buy","ord_type":"limit","price":"50.0","avg_price":"0.0","state":"cancel","origin_volume":"0.02","remaining_volume":"0.02","executed_volume":"0.0","at":1587673809,"created_at":1587673809,"updated_at":1587673827,"trades_count":0}}
    
    WS_ORDER_CANCEL_SELL_LIMIT = {"order":{"id":10000,"market":"fthusd","kind":"ask","side":"sell","ord_type":"limit","price":"50.0","avg_price":"0.0","state":"cancel","origin_volume":"0.02","remaining_volume":"0.02","executed_volume":"0.0","at":1587673809,"created_at":1587673809,"updated_at":1587673827,"trades_count":0}}
