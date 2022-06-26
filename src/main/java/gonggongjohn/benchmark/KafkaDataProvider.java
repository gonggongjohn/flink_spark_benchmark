package gonggongjohn.benchmark;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class KafkaDataProvider {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String kafkaTopic = "stream";
        String kafkaAddress = args[0];

        int event = Integer.parseInt(args[1]);
        double variance = Double.parseDouble(args[2]);

        Random random = new Random();
        Random randKey = new Random();
        List<Double> timeStamps = new ArrayList<>();
        double smallest = 0.0;
        for(int i = 0; i < event; i++) {
            double rand = random.nextGaussian() * variance;
            if(rand < smallest) smallest = rand;
            timeStamps.add(rand);
        }
        for(int i = 0; i < timeStamps.size(); i++){
            timeStamps.set(i, timeStamps.get(i) + Math.abs(smallest));
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);

        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 40; i++) {
            String sendValue = randKey.nextInt(100) + "," + System.currentTimeMillis();
            ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, sendValue);
            RecordMetadata sendMeta = producer.send(record).get();
            System.out.println("Producer message sent! Topic: " + sendMeta.topic() + ", Content: " + sendValue);
            Thread.sleep(randKey.nextInt(300));
        }
    }
}
