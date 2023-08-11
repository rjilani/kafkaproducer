import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

public class Driver {

    public static void main(String[] args) throws InterruptedException {

        String topicName = "quickstart-events";
        if (args.length == 1) {
            topicName = args[0];
        }


        System.out.println("Topic name is: " + topicName);
        Thread.sleep(2000);
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
//        props.put("delivery.timeout.ms", 30000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        try{
            for(int i = 101; i < 1000; i++){
                Thread.sleep(500);
                System.out.println(i);
//                String key = UUID.randomUUID().toString();

                String key;
                if(i % 2 == 0)
                    key = "chicago";
                else
                    key = "newyork";

                System.out.println(key);

                producer.send(new ProducerRecord(topicName, key , "test message - " + i ));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }

}
