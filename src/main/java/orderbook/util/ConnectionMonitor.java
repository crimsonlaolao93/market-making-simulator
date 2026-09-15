//package orderbook.util;
//
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.concurrent.locks.StampedLock;
//
//public class ConnectionMonitor {
//
//    private final AtomicLong lastMessageTime = new AtomicLong(System.nanoTime());
//    private final StampedLock lock = new StampedLock();
//    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
//    private volatile boolean isConnected = false;
//    private final long heartbeatIntervalMs;
//    private final long maxSilencePeridsMs;
//
//    public ConnectionMonitor(long heartbeatIntervalMs, long maxSilencePeridsMs) {
//        this.heartbeatIntervalMs = heartbeatIntervalMs;
//        this.maxSilencePeridsMs = maxSilencePeridsMs;
//    }
//
//    public void start(){
//        heartbeatExecutor.scheduleAtFixedRate(this::check);
//    }
//
//    public void recordMessage(){
//        lastMessageTime.set(System.nanoTime());
//        long stamp = lock.writeLock();
//        try{
//            isConnected = true;
//        }finally {
//            lock.unlockWrite(stamp);
//        }
//    }
//
//    public void recordDisconnect(){
//        long stamp = lock.writeLock();
//        try{
//            isConnected = false;
//        }finally {
//            lock.unlockWrite(stamp);
//        }
//    }
//
//
//}
