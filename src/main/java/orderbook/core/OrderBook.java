package orderbook.core;

import lombok.Data;
import orderbook.risk.RiskManager;
import orderbook.util.PerformanceMetrics;
import orderbook.util.TradePublisher;
import org.json.JSONArray;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

@Data
public class OrderBook {
    // Thread-safe price levels
    private final ConcurrentSkipListMap<Double, PriceLevel> bids;
    private final ConcurrentSkipListMap<Double, PriceLevel> asks;

    //Concurrency control
    private final StampedLock bookLock = new StampedLock();
    private final AtomicLong tradeIdGenerator = new AtomicLong(0);
    private final RollingVolatilityCalculator volatilityCalculator;

    //Dependencies
    private final String symbol;
    private final RiskManager riskManager;
    private final TradePublisher tradePublisher;

    public OrderBook(RiskManager riskManager, String symbol, TradePublisher tradePublisher){
        this.symbol = symbol;
        this.riskManager = riskManager;
        this.tradePublisher = tradePublisher;
        this.volatilityCalculator = new RollingVolatilityCalculator(100);

        // Bids: highest price first
        this.bids = new ConcurrentSkipListMap<>((a,b) -> Double.compare(b,a));
        // Asks: lowest price first
        this.asks = new ConcurrentSkipListMap<>();
    }

    // Volatility calculation (required by MarketMaker)
    public double getVolatility() {
        return volatilityCalculator.getVolatility();
    }

    // Core matching engine
    public void processOrder(Order order){
        long startTime = System.nanoTime();

        try{
            // Pre-trade risk check
            if(!riskManager.checkPosition(order.getSymbol(),
                    order.isBuy()? order.getQuantity(): -order.getQuantity())){
                rejectOrder(order, "Risk limit exceeded");
                return;
            }

            // Matching phase
            if(order.isBuy()){
                matchAgainstAsks(order);
            }else{
                matchAgainstBids(order);
            }

            // Add remaining to book
            if(order.getQuantity() > 0){
                addToOrderBook(order);
            }
        }finally {
            PerformanceMetrics.recordLatency(System.nanoTime()-startTime);
        }
    }

    private void matchAgainstAsks(Order buyOrder){
        Iterator<Map.Entry<Double, PriceLevel>> it = asks.entrySet().iterator();
        while(it.hasNext() && buyOrder.getQuantity()>0){
            Map.Entry<Double, PriceLevel> entry = it.next();
            if(entry.getKey() > buyOrder.getPrice()) break;

            entry.getValue().match(buyOrder,tradeIdGenerator,tradePublisher,riskManager);
            if(entry.getValue().isEmpty()){
                it.remove();
            }
        }
    }

    private void matchAgainstBids(Order sellOrder){
        Iterator<Map.Entry<Double, PriceLevel>> it = bids.entrySet().iterator();
        while(it.hasNext() && sellOrder.getQuantity()>0){
            Map.Entry<Double, PriceLevel> entry = it.next();
            if(entry.getKey()<sellOrder.getPrice()) break;

            entry.getValue().match(sellOrder,tradeIdGenerator,tradePublisher,riskManager);
            if(entry.getValue().isEmpty()){
                it.remove();
            }
        }
    }

    private void addToOrderBook(Order order){
        long stamp = bookLock.writeLock();
        try{
            ConcurrentSkipListMap<Double, PriceLevel> book = order.isBuy()? bids: asks;
            book.computeIfAbsent(order.getPrice(),
                    p -> new PriceLevel(p,1000)).addOrder(order);
        }finally {
            bookLock.unlockWrite(stamp);
        }
    }

    public Double getBestBid(){
        long stamp = bookLock.tryOptimisticRead();
        Double bid = bids.isEmpty()? null: bids.firstKey();
        if(!bookLock.validate(stamp)){
            stamp = bookLock.readLock();
            try{
                bid = bids.isEmpty()? null: bids.firstKey();
            }finally {
                bookLock.unlockRead(stamp);
            }
        }
        return bid;
    }

    public Double getBestAsk() {
        long stamp = bookLock.tryOptimisticRead();
        Double ask = asks.isEmpty() ? null : asks.firstKey();
        if (!bookLock.validate(stamp)) {
            stamp = bookLock.readLock();
            try {
                ask = asks.isEmpty() ? null : asks.firstKey();
            } finally {
                bookLock.unlockRead(stamp);
            }
        }
        return ask;
    }

    public double getMidPrice() {
        Double bestBid = getBestBid();
        Double bestAsk = getBestAsk();
        if (bestBid == null || bestAsk == null) {
            throw new IllegalStateException("No market data available");
        }
        return (bestBid + bestAsk) / 2;
    }

    public void cancelAllQuotes(String prefix){
        long stamp = bookLock.writeLock();
        try{
            bids.values().removeIf(level->level.cancelOrdersWithPrefix(prefix));
            asks.values().removeIf(level->level.cancelOrdersWithPrefix(prefix));
        }finally {
            bookLock.unlockWrite(stamp);
        }
    }

    /**
     * Processes real-time market data updates from the exchange
     * @param bids JSON array of bid updates [["price", "quantity"],...]
     * @param asks JSON array of ask updates [["price", "quantity"],...]
     */
    public void handleMarketDataUpdates(JSONArray bids, JSONArray asks) {
        long startTime = System.nanoTime();

        try {
            // Apply updates atomically
            long stamp = bookLock.writeLock();
            try {
                // Process bids
                for (int i = 0; i < bids.length(); i++) {
                    JSONArray bid = bids.getJSONArray(i);
                    double price = Double.parseDouble(bid.getString(0));
                    int size = (int) Double.parseDouble(bid.getString(1));

                    if (size > 0) {
                        this.bids.put(price, new PriceLevel(price, size));
                    } else {
                        this.bids.remove(price); // Remove empty levels
                    }
                }

                // Process asks
                for (int i = 0; i < asks.length(); i++) {
                    JSONArray ask = asks.getJSONArray(i);
                    double price = Double.parseDouble(ask.getString(0));
                    int size = (int) Double.parseDouble(ask.getString(1));

                    if (size > 0) {
                        this.asks.put(price, new PriceLevel(price, size));
                    } else {
                        this.asks.remove(price); // Remove empty levels
                    }
                }

                // Update volatility tracker
                volatilityCalculator.update(getMidPrice());
            } finally {
                bookLock.unlockWrite(stamp);
            }
        } catch (Exception e) {
            System.err.printf("Failed to process market data: %s%n", e.getMessage());
            // Consider adding recovery logic here
        } finally {
            PerformanceMetrics.recordLatency(System.nanoTime() - startTime);
        }
    }

    // Diagnostics
    public void printOrderBook(int levels) {
        System.out.println("\n=== " + symbol + " Order Book ===");
        System.out.printf("%-15s | %15s%n", "BID", "ASK");

        Iterator<PriceLevel> bidIt = bids.values().iterator();
        Iterator<PriceLevel> askIt = asks.values().iterator();

        for (int i = 0; i < levels; i++) {
            String bidStr = bidIt.hasNext() ? formatLevel(bidIt.next()) : "";
            String askStr = askIt.hasNext() ? formatLevel(askIt.next()) : "";
            System.out.printf("%-15s | %15s%n", bidStr, askStr);
        }
    }

    private String formatLevel(PriceLevel level) {
        return String.format("%.2f (%d)", level.getPrice(), level.getTotalQuantity());
    }

    private void rejectOrder(Order order, String reason){
        System.err.printf("Order %s rejected: %s%n", order.getOrderId(), reason);
    }

    // Supporting class for volatility calculation
    private static final class RollingVolatilityCalculator {
        private final double[] prices;
        private int index = 0;
        private boolean filled = false;

        public RollingVolatilityCalculator(int windowSize) {
            this.prices = new double[windowSize];
        }

        public synchronized void update(double price) {
            prices[index] = price;
            index = (index + 1) % prices.length;
            if (index == 0) filled = true;
        }

        public synchronized double getVolatility() {
            if (!filled && index < 2) return 0.0;

            int count = filled ? prices.length : index;
            double sum = 0.0;
            for (int i = 0; i < count; i++) {
                sum += prices[i];
            }
            double mean = sum / count;

            double variance = 0.0;
            for (int i = 0; i < count; i++) {
                variance += Math.pow(prices[i] - mean, 2);
            }
            variance /= count;

            return Math.sqrt(variance) / mean; // Return as percentage
        }
    }
}
