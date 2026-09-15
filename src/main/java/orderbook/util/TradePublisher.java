package orderbook.util;

import orderbook.core.Trade;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TradePublisher {

    // In-memory bus
    private final ExecutorService inMemoryBus = Executors.newSingleThreadExecutor();
    private final ConcurrentLinkedQueue<TradeListener> inMemoryListeners = new ConcurrentLinkedQueue();

    private final KafkaProducer<String, Trade> kafkaProducer;
    private final String  kafkaTopic;
    private final boolean kafkaEnabled;


    public TradePublisher(boolean enabledKafka, String kafkaBootstrapServers, String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
        this.kafkaEnabled = enabledKafka;

        if(kafkaEnabled){
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TradeSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
            this.kafkaProducer = new KafkaProducer<>(props);
        }else{
            this.kafkaProducer = null;
        }
    }

    public void addInMemoryListener(TradeListener listener){
        inMemoryListeners.add(listener);
    }

    public void publish(Trade trade){
        inMemoryBus.submit(()->{
            for(TradeListener listener: inMemoryListeners){
                try{
                    listener.onTrade(trade);
                }catch (Exception e){
                    System.err.println("Listener failed: " + e.getMessage());
                }
            }
        });

        if(kafkaEnabled){
            ProducerRecord<String, Trade> record = new ProducerRecord<>(
                    kafkaTopic,
                    trade.getSymbol(),
                    trade
            );

            kafkaProducer.send(record, (metadata,exception)->{
                if(exception!=null){
                    System.err.println("Kafka send failed: " + exception.getMessage());
                }
            });
        }
    }

    public void shutdown(){
        inMemoryBus.shutdown();
        if(kafkaProducer!=null) kafkaProducer.close();
    }

    public static class TradeSerializer implements Serializer<Trade>{
        @Override
        public byte[] serialize(String topic, Trade trade) {
            return JsonUtil.toJson(trade).getBytes(StandardCharsets.UTF_8);
        }
    }
}
