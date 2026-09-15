package orderbook.risk;

import orderbook.core.Trade;
import orderbook.util.TradeListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.locks.StampedLock;

public class RiskManager implements TradeListener {

    private final Map<String, Double> positionLimits = new ConcurrentHashMap<>();
    private final Map<String, DoubleAdder> currentPositions = new ConcurrentHashMap<>();

    private final DoubleAdder realizedPnl = new DoubleAdder();
    private final DoubleAdder unrealizedPnl = new DoubleAdder();

    private final AtomicReference<Boolean> stopLossTriggered = new AtomicReference<>(false);
    private final double maxDailyLoss;
    private final StampedLock pnlLock = new StampedLock();

    public RiskManager(double maxDailyLoss) {
        this.maxDailyLoss = maxDailyLoss;
    }

    // ---- Position Limits ----
    public void setPositionLimits(String symbol, double maxPosition){
        positionLimits.put(symbol, maxPosition);
        currentPositions.putIfAbsent(symbol, new DoubleAdder());
    }

    public boolean checkPosition(String symbol, double delta){
        Double limit = positionLimits.get(symbol);
        if(limit == null) return true;

        DoubleAdder current = currentPositions.get(symbol);
        double newPosition = current.doubleValue() + delta;

        return Math.abs(newPosition) <= limit;
    }

    public void updateOnTrade(Trade trade){
        double pnlImpact = trade.getQuantity() * trade.getPrice() * (trade.isBuyerMaker()?-1:1);
        realizedPnl.add(pnlImpact);
        updatePositions(trade.getSymbol(),trade.isBuyerMaker()? -trade.getQuantity(): trade.getQuantity());
        checkStopLoss();
    }

    private void updatePositions(String symbol, double delta){
        currentPositions.computeIfAbsent(symbol, k-> new DoubleAdder()).add(delta);
    }

    // ---- Circuit Breakers ----
    private void checkStopLoss() {
        if (realizedPnl.doubleValue() < -maxDailyLoss) {
            long stamp = pnlLock.writeLock();
            try {
                // Double-check with lock
                if (realizedPnl.doubleValue() < -maxDailyLoss) {
                    stopLossTriggered.set(true);
                    System.err.println("🚨 STOP-LOSS TRIGGERED! PnL: " + realizedPnl.doubleValue());
                }
            } finally {
                pnlLock.unlockWrite(stamp);
            }
        }
    }

    // ---- Thread-Safe Getters ----
    public double getRealizedPnl() {
        return realizedPnl.doubleValue();
    }

    public double getPosition(String symbol) {
        DoubleAdder position = currentPositions.get(symbol);
        return position != null ? position.doubleValue() : 0.0;
    }

    public boolean isStopLossActive() {
        return stopLossTriggered.get();
    }

    @Override
    public void onTrade(Trade trade) {
        // realtime exposure updates
        System.out.printf("[RISK] %s %d @ %.2f (Pnl: %.2f)%n",
                trade.getSymbol(),
                trade.getQuantity(),
                trade.getPrice());
    }
}
