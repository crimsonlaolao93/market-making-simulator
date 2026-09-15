package orderbook.exchange;

import orderbook.util.PerformanceMetrics;
import orderbook.core.OrderBook;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BinanceWebSocketClient extends WebSocketClient {

    private final OrderBook orderBook;
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private final ExchangeHealthMonitor exchangeHealthMonitor;
    private final AtomicBoolean reconnectFlag = new AtomicBoolean(true);
    private final ScheduledExecutorService reconnectorExecutor = Executors.newSingleThreadScheduledExecutor();
    private int reconnectAttempts = 0;

    public BinanceWebSocketClient(URI serverUri, OrderBook orderBook, ExchangeHealthMonitor exchangeHealthMonitor) {
        super(serverUri);
        this.orderBook = orderBook;
        this.exchangeHealthMonitor = exchangeHealthMonitor;
        new Thread(this::processTasks).start();
    }

    private void processTasks(){
        while(!Thread.currentThread().isInterrupted()){
            try{
                taskQueue.take().run();
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        System.out.println("Connected to Binance WebSocket");
        reconnectAttempts = 0;
    }

    @Override
    public void onMessage(String message) {
        // DEBUG: Log raw message (first 200 chars)
        System.out.println("[DEBUG] RCVD: " + message.substring(0, Math.min(200, message.length())));
        exchangeHealthMonitor.recordMessage(); // Count ALL messages

        try {
            JSONObject json = new JSONObject(message);
            if (json.has("e") && "depthUpdate".equals(json.getString("e"))) {
                long startTime = System.nanoTime();

                JSONArray bids = json.getJSONArray("b");
                JSONArray asks = json.getJSONArray("a");
                orderBook.handleMarketDataUpdates(bids, asks);

                exchangeHealthMonitor.recordTrade();
                PerformanceMetrics.recordLatency(System.nanoTime() - startTime);
            }
        } catch (Exception e) {
            System.err.println("Message processing failed: " + e.getMessage());
        }
    }

    @Override
    public void onClose(int i, String reason, boolean b) {
        System.out.println("Disconnected: " + reason);
        if (reconnectFlag.get()) scheduleReconnect();
    }

    @Override
    public void onError(Exception ex) {
        System.err.println("WebSocket error: " + ex.getMessage());
    }

    private void scheduleReconnect(){
        long delay = (long) Math.min(5000, 100*Math.pow(2, reconnectAttempts++));
        reconnectorExecutor.schedule(()->{
            System.out.println("Reconnecting (attempt " + reconnectAttempts + ")...");
            this.connect();
        }, delay, TimeUnit.MILLISECONDS);
    }

    public void shutdown(){
        reconnectFlag.set(false);
        reconnectorExecutor.shutdown();
    }

}
