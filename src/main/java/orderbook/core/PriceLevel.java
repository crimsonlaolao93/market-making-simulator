package orderbook.core;

import lombok.Data;
import orderbook.risk.RiskManager;
import orderbook.util.TradePublisher;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.StampedLock;

@Data
public class PriceLevel {

    private final double price;
    private final AtomicReferenceArray<Order> orders;
    private final AtomicInteger head = new AtomicInteger(0);
    private final AtomicInteger tail = new AtomicInteger(0);
    private final AtomicInteger totalQuantity = new AtomicInteger(0);
    private final StampedLock levelLock = new StampedLock();
    private final int capacity;

    public PriceLevel(double price, int capacity) {
        this.price = price;
        this.orders = new AtomicReferenceArray<>(capacity);
        this.capacity = capacity;
    }

    // Thread-safe order addition
    public boolean addOrder(Order order){
        long stamp = levelLock.writeLock();
        try{
            int currentTail = tail.get();
            if(currentTail >= capacity){
                return false;
            }
            orders.set(currentTail, order);
            tail.incrementAndGet();
            totalQuantity.addAndGet(order.getQuantity());
            return true;
        }finally {
            levelLock.unlockWrite(stamp);
        }
    }

    // Core matching logic
    public void match(Order incomingOrder, AtomicLong tradeIdGenerator,
                      TradePublisher tradePublisher, RiskManager riskManager){
        int currentHead = head.get();
        while(currentHead < tail.get() && incomingOrder.getQuantity()>0){
            Order restingOrder = orders.get(currentHead);

            if(restingOrder == null || restingOrder.getQuantity() == 0){
                head.compareAndSet(currentHead, currentHead+1);
                currentHead = head.get();
                continue;
            }
            int fillQty = Math.min(restingOrder.getQuantity(), incomingOrder.getQuantity());
            if(fillQty>0){
                executeTrade(incomingOrder, restingOrder, fillQty, tradeIdGenerator, tradePublisher, riskManager);

                if(restingOrder.getQuantity() == 0){
                    head.compareAndSet(currentHead, currentHead+1);
                }
            }
            currentHead = head.get();
        }
    }

    private void executeTrade(Order incoming, Order resting, int fillQty,
                              AtomicLong tradeIdGenerator, TradePublisher publisher,
                              RiskManager riskManager){
        // Update quantities
        resting.decreaseQuantity(fillQty);
        incoming.decreaseQuantity(fillQty);
        totalQuantity.addAndGet(-fillQty);

        // Create trade
        Trade trade = new Trade(
                "TR-" + tradeIdGenerator.incrementAndGet(),
                resting.getSymbol(),
                resting.getPrice(),
                fillQty,
                incoming.isBuy(),
                incoming.getOrderId(),
                incoming.getOrderId()
        );

        // Notify systems
        publisher.publish(trade);
        riskManager.updateOnTrade(trade);
    }

    // In PriceLevel.java
    public boolean cancelOrdersWithPrefix(String prefix) {
        boolean cancelled = false;
        for (int i = head.get(); i < tail.get(); i++) {
            Order order = orders.get(i);
            if (order != null && order.getOrderId().startsWith(prefix)) {
                orders.set(i, null);
                totalQuantity.addAndGet(-order.getQuantity());
                cancelled = true;
            }
        }
        return cancelled;
    }

    // Order removal (e.g., cancellations)
    public boolean removeOrder(String orderId) {
        long stamp = levelLock.writeLock();
        try {
            for (int i = head.get(); i < tail.get(); i++) {
                Order order = orders.get(i);
                if (order != null && order.getOrderId().equals(orderId)) {
                    int qty = order.getQuantity();
                    orders.set(i, null);
                    totalQuantity.addAndGet(-qty);
                    return true;
                }
            }
            return false;
        } finally {
            levelLock.unlockWrite(stamp);
        }
    }

    public boolean isEmpty(){
        return head.get() >= tail.get() || totalQuantity.get() == 0;
    }

    // Debugging
    @Override
    public String toString() {
        return String.format("PriceLevel{price=%.2f, qty=%d, orders=%d/%d}",
                price, totalQuantity.get(), tail.get() - head.get(), capacity);
    }
}
