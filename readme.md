# Market Making Simulator

[![Java](https://img.shields.io/badge/Java-17+-blue?logo=openjdk)](https://openjdk.org/)
[![Concurrency](https://img.shields.io/badge/Concurrency-High--Performance-green)](https://docs.oracle.com/javase/tutorial/essential/concurrency/)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

A high-performance Java market-making engine simulating real-world trading strategies with risk controls, order book dynamics, and exchange connectivity.

![Market Making Flow Diagram](https://i.imgur.com/JZQm4fP.png) *(Example diagram - replace with your actual architecture)*

## Key Features

- **Core Engine**
    - Event-driven order matching with price-time priority
    - Multi-threaded quote generation (100μs latency)
    - Lock-free order book updates using `StampedLock` and `AtomicReference`

- **Risk Management**
    - Real-time PnL tracking with `DoubleAdder`
    - Circuit breaker pattern for trading halts
    - Position limits per asset (`ConcurrentHashMap`)

- **Exchange Integration**
    - Binance WebSocket feed (real-time depth updates)
    - FIX protocol order gateway (QuickFIX/J)
    - Exponential backoff reconnection logic

- **Trading Strategies**
    - Dynamic spread calculation (volatility + inventory skew)
    - Inventory rebalancing thresholds
    - Configurable minimum spread (% of mid-price)

## Technical Highlights

```java
// Example: Thread-safe spread calculation
public Quote calculateQuote(double midPrice, double volatility, double inventorySkew) {
    double spread = Math.max(
        baseSpreadPct + (volatility * volatilitySensitivity),
        minSpreadPct
    );
    return new Quote(
        roundToTick(midPrice * (1 - spread)),
        adjustSizeByVolatility(defaultSize, volatility),
        roundToTick(midPrice * (1 + spread)),
        adjustSizeByVolatility(defaultSize, volatility)
    );
}