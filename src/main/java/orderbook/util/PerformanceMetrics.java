package orderbook.util;

import java.util.concurrent.atomic.LongAdder;

public class PerformanceMetrics {

    private static final LongAdder totalLatency = new LongAdder();
    private static final LongAdder count = new LongAdder();
    private static final LongAdder tradeCount = new LongAdder();

    public static void recordTrade() {
        tradeCount.increment();
    }

    public static long getTradeCount() {
        return tradeCount.sum();
    }

    public static double getAvgLatencyMicros() {
        return count.sum() > 0 ? totalLatency.sum() / (double) count.sum() / 1000 : 0;
    }

    public static void recordLatency(long latencyNs){
        totalLatency.add(latencyNs);
        count.increment();
    }

    public static void printStats() {
        System.out.printf("""
            ================
            OrderBook Metrics
            ================
            Avg Latency: %.2f μs
            Total Orders: %d
            """,
                totalLatency.sum() / (double) count.sum() / 1000,
                count.sum()
        );
    }
}
