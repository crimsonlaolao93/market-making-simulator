package orderbook;

import orderbook.core.OrderBook;
import orderbook.core.Trade;
import orderbook.exchange.BinanceWebSocketClient;
import orderbook.exchange.ExchangeHealthMonitor;
import orderbook.risk.RiskManager;
import orderbook.strategy.InventoryManager;
import orderbook.strategy.MarketMaker;
import orderbook.strategy.SpreadCalculator;
import orderbook.util.Config;
import orderbook.util.PerformanceMetrics;
import orderbook.util.TradeListener;
import orderbook.util.TradePublisher;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class App {
    private static final String KAFKA_BOOT_SERVERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "trades";
    private static final boolean ENABLE_KAFKA = false;
    private static final String SYMBOL = "BTCUSDT";
    private static final double MAX_DAILY_LOSS = 10_000.0;
    private static final int ORDER_SIZE = 100;
    private static final double MIN_SPREAD_PCT = 0.0005;

    public static void main(String[] args) throws Exception {

        // 1. Initialize core components
        TradePublisher tradePublisher = new TradePublisher(ENABLE_KAFKA,KAFKA_BOOT_SERVERS,KAFKA_TOPIC);

        RiskManager riskManager = new RiskManager(MAX_DAILY_LOSS);
        OrderBook orderBook = new OrderBook(riskManager, SYMBOL,tradePublisher);

        // 2. Configure market maker
        SpreadCalculator spreadCalculator = new SpreadCalculator(
                0.001,
                2.0,
                0.3,
                0.0005
        );

        InventoryManager inventoryManager = new InventoryManager(
                0.0,
                10.0,
                5_000.0
        );

        MarketMaker marketMaker = new MarketMaker(
                orderBook,
                riskManager,
                spreadCalculator,
                inventoryManager,
                SYMBOL,
                100,    // default order size
                0.0005 // min spread
        );

        // 3. Set up exchange connection
        ExchangeHealthMonitor healthMonitor = new ExchangeHealthMonitor();
        BinanceWebSocketClient wsClient = new BinanceWebSocketClient(
                new URI("wss://stream.binance.com:9443/ws/" + SYMBOL.toLowerCase() + "@depth"),
                orderBook,
                healthMonitor
        );

        // 4. Add monitoring
        ScheduledExecutorService monitorExecutor = Executors.newSingleThreadScheduledExecutor();
        monitorExecutor.scheduleAtFixedRate(() -> {
            System.out.println("\n=== System Status ===");
            System.out.printf("Spread: %.4f%% | PnL: %.2f USD%n",
                    (orderBook.getBestAsk() - orderBook.getBestBid()) / orderBook.getMidPrice() * 100,
                    riskManager.getRealizedPnl());

            System.out.printf("Inventory Skew: %.2f | Health: %d msg/s%n",
                    inventoryManager.getSkewFactor(SYMBOL),
                    healthMonitor.getMessagesPerSecond());

//            System.out.printf("Trade Publisher: %d failed (Kafka: %d)%n",
//                    tradePublisher.getFailedListeners(),
//                    tradePublisher.getFailedKafkaSends());

            orderBook.printOrderBook(5);
        }, 1, 1, TimeUnit.SECONDS);

        // 5. Start components
        wsClient.connect();
        marketMaker.start();

        // 6. Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            marketMaker.shutdown();
            wsClient.close();
            tradePublisher.shutdown();
            monitorExecutor.shutdown();
            try {
                if (!monitorExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                    monitorExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            PerformanceMetrics.printStats();
        }));

        // 7. Keep main thread alive
        new CountDownLatch(1).await();
    }

    // Simple trade logger
    private static class TradeLogger implements TradeListener {
        @Override
        public void onTrade(Trade trade) {
            System.out.printf("[TRADE] %s %d @ %.2f (Value: %.2f)%n",
                    trade.getSymbol(),
                    trade.getQuantity(),
                    trade.getPrice(),
                    trade.getQuantity() * trade.getPrice());
        }
    }
}