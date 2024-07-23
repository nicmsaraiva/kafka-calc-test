package nicmsaraiva.com.integration;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaMessageConsumer {
    private final Consumer<String, String> consumer;
    private final Producer<String, String> producer;
    private final String inputTopic;
    private final String outputTopic;

    public KafkaMessageConsumer(String inputTopic, String outputTopic) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "calculator-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<>(consumerProps);
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.consumer.subscribe(Collections.singletonList(inputTopic));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(producerProps);
    }

    public void consumeAndProcessMessages() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed message: key = %s, value = %s%n", record.key(), record.value());
                String result = processMessage(record.value());
                sendResult(record.key(), result);
            }
        }
    }

    private String processMessage(String message) {
        String[] parts = message.split(":");
        String operation = parts[0];
        int num1 = Integer.parseInt(parts[1]);
        int num2 = Integer.parseInt(parts[2]);

        int result;
        switch (operation) {
            case "add":
                result = num1 + num2;
                break;
            case "subtract":
                result = num1 - num2;
                break;
            case "multiply":
                result = num1 * num2;
                break;
            case "divide":
                result = num1 / num2;
                break;
            default:
                throw new IllegalArgumentException("Invalid operation: " + operation);
        }

        return String.valueOf(result);
    }

    private void sendResult(String key, String result) {
        ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, key, result);
        producer.send(record);
    }
}
