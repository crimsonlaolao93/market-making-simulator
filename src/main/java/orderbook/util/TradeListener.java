package orderbook.util;

import orderbook.core.Trade;

public interface TradeListener {

    void onTrade(Trade trade);
}
