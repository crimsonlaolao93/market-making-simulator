package orderbook.strategy;

import orderbook.core.*;
import orderbook.risk.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.StampedLock;

public final class MarketMaker {
    // Dependencies
    private final OrderBook orderBook;
    private final RiskManager riskManager;
    private final SpreadCalculator spreadCalculator;
    private final InventoryManager inventoryManager;

    // Configuration
    private final String symbol;
    private final int defaultOrderSize;
    private final double minSpreadPct;

    // State
    private final AtomicReference<MarketMakerState> state = new AtomicReference<>(MarketMakerState.ACTIVE);
    private final StampedLock quoteLock = new StampedLock();
    private final ScheduledExecutorService executor;

    public enum MarketMakerState { ACTIVE, PAUSED, STOPPED }

    public MarketMaker(OrderBook orderBook,
                       RiskManager riskManager,
                       SpreadCalculator spreadCalculator,
                       InventoryManager inventoryManager,
                       String symbol,
                       int defaultOrderSize,
                       double minSpreadPct) {
        this.orderBook = orderBook;
        this.riskManager = riskManager;
        this.spreadCalculator = spreadCalculator;
        this.inventoryManager = inventoryManager;
        this.symbol = symbol;
        this.defaultOrderSize = defaultOrderSize;
        this.minSpreadPct = minSpreadPct;
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactory() {
                    private final AtomicInteger count = new AtomicInteger(0);
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "market-maker-" + count.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    }
                }
        );
    }

    public void start() {
        executor.scheduleAtFixedRate(this::updateQuotes, 0, 100, TimeUnit.MILLISECONDS);
    }

    private void updateQuotes() {
        if (state.get() != MarketMakerState.ACTIVE) return;

        try {
            // 1. Cancel existing quotes
            cancelOldQuotes();

            // 2. Check circuit breakers
            if (riskManager.isStopLossActive()) {
                pause("Stop loss triggered");
                return;
            }

            // 3. Calculate new quotes
            Quote newQuote = calculateNewQuote();
            if (newQuote == null) return;

            // 4. Post new quotes
            postNewQuotes(newQuote);
        } catch (Exception e) {
            System.err.printf("[%s] Market making error: %s%n",
                    System.currentTimeMillis(), e.getMessage());
        }
    }

    private void cancelOldQuotes() {
        long stamp = quoteLock.writeLock();
        try {
            orderBook.cancelAllQuotes("MM-" + symbol + "-");
        } finally {
            quoteLock.unlockWrite(stamp);
        }
    }

    private Quote calculateNewQuote() {
        Double bestBid = orderBook.getBestBid();
        Double bestAsk = orderBook.getBestAsk();

        if (bestBid == null || bestAsk == null) {
            System.out.println("Waiting for market data...");
            return null;
        }

        double midPrice = (bestBid + bestAsk) / 2;
        double inventorySkew = inventoryManager.getSkewFactor(symbol);
        double volatility = orderBook.getVolatility();

        return spreadCalculator.calculateQuote(
                midPrice,
                volatility,
                inventorySkew,
                minSpreadPct,
                defaultOrderSize
        );
    }

    private void postNewQuotes(Quote quote) {
        long stamp = quoteLock.writeLock();
        try {
            // Post bid
            if (riskManager.checkPosition(symbol, quote.bidSize())) {
                Order bid = new Order(
                        "MM-" + symbol + "-BID-" + System.nanoTime(),
                        true,
                        quote.bidPrice(),
                        quote.bidSize(),
                        symbol
                );
                orderBook.processOrder(bid);
            }

            // Post ask
            if (riskManager.checkPosition(symbol, -quote.askSize())) {
                Order ask = new Order(
                        "MM-" + symbol + "-ASK-" + System.nanoTime(),
                        false,
                        quote.askPrice(),
                        quote.askSize(),
                        symbol
                );
                orderBook.processOrder(ask);
            }
        } finally {
            quoteLock.unlockWrite(stamp);
        }
    }

    public void pause(String reason) {
        state.set(MarketMakerState.PAUSED);
        cancelOldQuotes();
        System.out.printf("Market making paused: %s%n", reason);
    }

    public void resume() {
        state.set(MarketMakerState.ACTIVE);
        System.out.println("Market making resumed");
    }

    public void shutdown() {
        state.set(MarketMakerState.STOPPED);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        cancelOldQuotes();
        System.out.println("Market maker shutdown complete");
    }

    public MarketMakerState getState() {
        return state.get();
    }

    // Records for immutable data
    public record Quote(double bidPrice, int bidSize, double askPrice, int askSize) {}
    public record MarketMakerConfig(
            String symbol,
            int defaultOrderSize,
            double minSpreadPct,
            long quoteUpdateIntervalMs
    ) {}
}