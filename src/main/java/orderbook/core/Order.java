package orderbook.core;

import lombok.Data;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

@Data
public final class Order {
    // Getters (no setters for immutability except quantity)
    @Getter
    private final String orderId;
    private final boolean isBuy;
    private final double price;
    private final AtomicInteger quantity;  // Now atomic for thread safety
    private final long timestamp;
    private final String symbol;          // Added for multi-asset support

    // Constructor
    public Order(String orderId, boolean isBuy, double price, int quantity, String symbol) {
        this.orderId = orderId;
        this.isBuy = isBuy;
        this.price = price;
        this.quantity = new AtomicInteger(quantity);
        this.timestamp = System.nanoTime();
        this.symbol = symbol;
    }

    public int getQuantity() { return quantity.get(); }  // Atomic read

    // Thread-safe quantity reduction
    public void decreaseQuantity(int amount) {
        quantity.addAndGet(-amount);  // Atomic operation
    }

    // Builder pattern for order modifications
    public static class Builder {
        private String orderId;
        private boolean isBuy;
        private double price;
        private int quantity;
        private String symbol;

        public Builder(Order original) {
            this.orderId = original.orderId;
            this.isBuy = original.isBuy;
            this.price = original.price;
            this.quantity = original.getQuantity();
            this.symbol = original.symbol;
        }

        public Builder setQuantity(int newQuantity) {
            this.quantity = newQuantity;
            return this;
        }

        public Order build() {
            return new Order(orderId, isBuy, price, quantity, symbol);
        }
    }
}