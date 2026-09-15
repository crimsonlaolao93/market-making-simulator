package orderbook.strategy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.DoubleSupplier;

public final class InventoryManager {
    private final double targetInventory;
    private final double maxInventory;
    private final double rebalanceThreshold;

    // Thread-safe position tracking with DoubleAdder
    private final Map<String, DoubleAdder> positions = new ConcurrentHashMap<>();

    public InventoryManager(double targetInventory,
                            double maxInventory,
                            double rebalanceThreshold) {
        this.targetInventory = targetInventory;
        this.maxInventory = maxInventory;
        this.rebalanceThreshold = rebalanceThreshold;
    }

    public void updatePosition(String symbol, double delta) {
        positions.computeIfAbsent(symbol, k -> new DoubleAdder()).add(delta);
    }

    public double getSkewFactor(String symbol) {
        DoubleAdder position = positions.get(symbol);
        if (position == null) return 0.0;

        double current = position.sum();
        if (Math.abs(current) > maxInventory) {
            return Math.signum(current); // Max skew if over limit
        }
        return (current - targetInventory) / maxInventory; // Normalized [-1, 1]
    }

    public boolean needsRebalancing(String symbol, DoubleSupplier priceSupplier) {
        DoubleAdder position = positions.get(symbol);
        if (position == null) return false;

        double current = position.sum();
        double imbalance = Math.abs(current - targetInventory);
        return (imbalance * priceSupplier.getAsDouble()) > rebalanceThreshold;
    }

    public double getRebalancingQuantity(String symbol) {
        DoubleAdder position = positions.get(symbol);
        return position != null ? targetInventory - position.sum() : 0.0;
    }

    public void resetPosition(String symbol) {
        DoubleAdder adder = positions.get(symbol);
        if (adder != null) {
            adder.reset();
            adder.add(targetInventory);
        }
    }

    // Java 16+ record for configuration
    public record InventoryConfig(
            double targetInventory,
            double maxInventory,
            double rebalanceThreshold
    ) {
        public InventoryManager create() {
            return new InventoryManager(targetInventory, maxInventory, rebalanceThreshold);
        }
    }
}