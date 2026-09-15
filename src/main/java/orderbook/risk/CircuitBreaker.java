package orderbook.risk;

import java.time.Instant;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.StampedLock;
import java.util.function.DoubleSupplier;

public final class CircuitBreaker {
    // Threshold configurations (immutable)
    private final double maxDailyLoss;
    private final double maxPositionRisk;
    private final double volatilityThreshold;

    // State tracking (thread-safe)
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final DoubleAdder realizedPnl = new DoubleAdder();
    private final DoubleAdder maxDrawdown = new DoubleAdder();
    private final AtomicReference<Instant> lastTriggerTime = new AtomicReference<>();

    // Concurrency control
    private final StampedLock lock = new StampedLock();
    private final AtomicInteger consecutiveTriggers = new AtomicInteger(0);

    // Recovery strategy (Java 17 sealed interface)
    private final RecoveryStrategy recoveryStrategy;

    public enum State { CLOSED, OPEN, HALF_OPEN }

    // Sealed interface for recovery strategies
    public sealed interface RecoveryStrategy permits ExponentialBackoff, LinearBackoff {
        long getNextDelay(int triggerCount);
    }

    // Record-based implementations (Java 17)
    public record ExponentialBackoff(long baseMillis) implements RecoveryStrategy {
        public long getNextDelay(int triggerCount) {
            return (long) Math.pow(2, triggerCount - 1) * baseMillis;
        }
    }

    public record LinearBackoff(long incrementMillis) implements RecoveryStrategy {
        public long getNextDelay(int triggerCount) {
            return triggerCount * incrementMillis;
        }
    }

    public CircuitBreaker(double maxDailyLoss,
                          double maxPositionRisk,
                          double volatilityThreshold) {
        this(maxDailyLoss, maxPositionRisk, volatilityThreshold,
                new ExponentialBackoff(1000));
    }

    public CircuitBreaker(double maxDailyLoss,
                          double maxPositionRisk,
                          double volatilityThreshold,
                          RecoveryStrategy recoveryStrategy) {
        this.maxDailyLoss = maxDailyLoss;
        this.maxPositionRisk = maxPositionRisk;
        this.volatilityThreshold = volatilityThreshold;
        this.recoveryStrategy = recoveryStrategy;
    }

    // Fast-path check (lock-free)
    public boolean checkAllowTrade() {
        return switch (state.get()) {
            case CLOSED, HALF_OPEN -> true;
            case OPEN -> false;
        };
    }

    // Detailed risk assessment
    public boolean checkAllowTrade(DoubleSupplier volatilitySupplier,
                                   DoubleSupplier positionRiskSupplier) {
        if (!checkAllowTrade()) return false;

        long stamp = lock.tryOptimisticRead();
        double currentPnl = realizedPnl.sum();
        double currentVolatility = volatilitySupplier.getAsDouble();
        double currentPositionRisk = positionRiskSupplier.getAsDouble();

        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                currentPnl = realizedPnl.sum();
                currentVolatility = volatilitySupplier.getAsDouble();
                currentPositionRisk = positionRiskSupplier.getAsDouble();
            } finally {
                lock.unlockRead(stamp);
            }
        }

        return !shouldTrigger(currentPnl, currentVolatility, currentPositionRisk);
    }

    // Thread-safe metrics update
    public void updateMetrics(double pnlDelta,
                              double currentVolatility,
                              double positionRiskRatio) {
        long stamp = lock.writeLock();
        try {
            double newPnl = realizedPnl.sum() + pnlDelta;
            realizedPnl.add(pnlDelta);

            // Track maximum drawdown
            if (newPnl < maxDrawdown.sum()) {
                maxDrawdown.reset();
                maxDrawdown.add(newPnl);
            }

            if (shouldTrigger(newPnl, currentVolatility, positionRiskRatio)) {
                triggerCircuitBreaker();
            } else if (state.get() == State.HALF_OPEN) {
                consecutiveTriggers.set(0);
                state.set(State.CLOSED);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    private boolean shouldTrigger(double currentPnl,
                                  double volatility,
                                  double positionRisk) {
        return currentPnl <= -maxDailyLoss ||
                positionRisk > maxPositionRisk ||
                volatility > volatilityThreshold;
    }

    private void triggerCircuitBreaker() {
        state.set(State.OPEN);
        lastTriggerTime.set(Instant.now());
        int triggers = consecutiveTriggers.incrementAndGet();

        // Schedule recovery with configured strategy
        scheduleRecoveryCheck(triggers);
    }

    private void scheduleRecoveryCheck(int triggerCount) {
        new Thread(() -> {
            try {
                Thread.sleep(recoveryStrategy.getNextDelay(triggerCount));

                long stamp = lock.writeLock();
                try {
                    if (state.get() == State.OPEN) {
                        state.set(State.HALF_OPEN);
                    }
                } finally {
                    lock.unlockWrite(stamp);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "CircuitBreaker-Recovery").start();
    }

    // Java 17 pattern matching for state checks
    public String getStatusMessage() {
        return switch (state.get()) {
            case CLOSED -> "Operational (PnL: %.2f)".formatted(realizedPnl.sum());
            case HALF_OPEN -> "Probation Mode (Recovery in progress)";
            case OPEN -> "Trading Halted (Max drawdown: %.2f)".formatted(maxDrawdown.sum());
        };
    }

    // Records for immutable configuration
    public record CircuitBreakerConfig(
            double maxDailyLoss,
            double maxPositionRisk,
            double volatilityThreshold,
            RecoveryStrategy recoveryStrategy
    ) {}

    // Factory method
    public static CircuitBreaker fromConfig(CircuitBreakerConfig config) {
        return new CircuitBreaker(
                config.maxDailyLoss(),
                config.maxPositionRisk(),
                config.volatilityThreshold(),
                config.recoveryStrategy()
        );
    }
}