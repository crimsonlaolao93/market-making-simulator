package orderbook.strategy;

import java.util.concurrent.atomic.*;

public final class SpreadCalculator {
    // Configuration
    private final double baseSpreadPct;
    private final double volatilitySensitivity;
    private final double inventorySensitivity;
    private final double minSpreadPct;

    // State tracking
    private final AtomicReference<Double> lastVolatility = new AtomicReference<>(0.0);
    private final AtomicReference<Double> lastSkew = new AtomicReference<>(0.0);

    public SpreadCalculator(double baseSpreadPct,
                            double volatilitySensitivity,
                            double inventorySensitivity,
                            double minSpreadPct) {
        this.baseSpreadPct = baseSpreadPct;
        this.volatilitySensitivity = volatilitySensitivity;
        this.inventorySensitivity = inventorySensitivity;
        this.minSpreadPct = minSpreadPct;
    }

    public MarketMaker.Quote calculateQuote(double midPrice,
                                            double currentVolatility,
                                            double inventorySkew,
                                            double minSpread,
                                            int defaultSize) {
        // Update state
        lastVolatility.set(currentVolatility);
        lastSkew.set(inventorySkew);

        // Calculate spread components
        double volatilityComponent = calculateVolatilitySpread(currentVolatility);
        double inventoryComponent = calculateInventorySpread(inventorySkew);

        // Total spread adjustment
        double totalSpreadPct = Math.max(
                baseSpreadPct + volatilityComponent + inventoryComponent,
                minSpread
        );

        // Apply skew (widen the side we want to reduce)
        double bidSpread = totalSpreadPct * (1 + inventorySkew * 0.5);
        double askSpread = totalSpreadPct * (1 - inventorySkew * 0.5);

        // Calculate prices
        double bidPrice = roundToTick(midPrice * (1 - bidSpread));
        double askPrice = roundToTick(midPrice * (1 + askSpread));

        return new MarketMaker.Quote(
                bidPrice,
                adjustSizeByVolatility(defaultSize, currentVolatility),
                askPrice,
                adjustSizeByVolatility(defaultSize, currentVolatility)
        );
    }

    private double calculateVolatilitySpread(double volatility) {
        return volatility * volatilitySensitivity;
    }

    private double calculateInventorySpread(double skew) {
        return Math.abs(skew) * inventorySensitivity;
    }

    private int adjustSizeByVolatility(int baseSize, double volatility) {
        // Reduce size during high volatility
        double multiplier = 1.0 / (1.0 + volatility * 2);
        return (int) Math.max(1, baseSize * multiplier);
    }

    private double roundToTick(double price) {
        // Assuming 0.01 tick size for crypto
        return Math.round(price * 100) / 100.0;
    }

    // Record for configuration
    public record SpreadConfig(
            double baseSpreadPct,
            double volatilitySensitivity,
            double inventorySensitivity,
            double minSpreadPct
    ) {}

    public static SpreadCalculator fromConfig(SpreadConfig config) {
        return new SpreadCalculator(
                config.baseSpreadPct(),
                config.volatilitySensitivity(),
                config.inventorySensitivity(),
                config.minSpreadPct()
        );
    }
}