package orderbook.exchange;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

public class ExchangeHealthMonitor {
    private final StampedLock lock = new StampedLock();
    private volatile long lastMessageNanos = System.nanoTime();
    public final AtomicLong totalMessages = new AtomicLong(0);
    private final AtomicLong tradeEvents = new AtomicLong(0);

    // Called from WebSocket thread
    public void recordMessage() {
        long stamp = lock.writeLock();
        try {
            totalMessages.incrementAndGet();
            lastMessageNanos = System.nanoTime(); // volatile write
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    // Called from WebSocket thread
    public void recordTrade() {
        tradeEvents.incrementAndGet();
    }

    // Called from main thread
    public double getMessagesPerSecond() {
        long stamp = lock.tryOptimisticRead();
        long currentLastMsg = lastMessageNanos; // volatile read
        long currentTotal = totalMessages.get();

        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                currentLastMsg = lastMessageNanos;
                currentTotal = totalMessages.get();
            } finally {
                lock.unlockRead(stamp);
            }
        }

        if (currentTotal == 0) return 0.0;

        double elapsedSeconds = (System.nanoTime() - currentLastMsg) / 1_000_000_000.0;
        return elapsedSeconds > 0 ? currentTotal / elapsedSeconds : 0;
    }

    // Called from main thread
    public long getTradeCount() {
        return tradeEvents.get(); // AtomicLong is already thread-safe
    }
}