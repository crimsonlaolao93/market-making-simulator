package orderbook.core;

import lombok.Getter;
import java.time.Instant;

@Getter
public final class Trade {
    private final String tradeId;          // Unique trade ID (e.g., exchange-provided or UUID)
    private final String symbol;          // Trading pair (e.g., "BTCUSDT")
    private final double price;           // Execution price
    private final int quantity;          // Filled quantity
    private final boolean isBuyerMaker;   // Was the buyer the maker?
    private final Instant timestamp;      // Execution time
    private final String buyOrderId;      // Counterparty order IDs
    private final String sellOrderId;

    public Trade(String tradeId, String symbol, double price, int quantity,
                 boolean isBuyerMaker, String buyOrderId, String sellOrderId) {
        this.tradeId = tradeId;
        this.symbol = symbol;
        this.price = price;
        this.quantity = quantity;
        this.isBuyerMaker = isBuyerMaker;
        this.timestamp = Instant.now();
        this.buyOrderId = buyOrderId;
        this.sellOrderId = sellOrderId;
    }

    // Example method to calculate notional value
    public double getNotionalValue() {
        return price * quantity;
    }
}