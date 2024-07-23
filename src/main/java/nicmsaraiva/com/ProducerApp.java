package nicmsaraiva.com;

import nicmsaraiva.com.integration.KafkaMessageProducer;

public class ProducerApp {
    public static void main( String[] args )
    {
        KafkaMessageProducer producer = new KafkaMessageProducer("input_topic");

        producer.sendMessage("1", "add:3:5");
        producer.sendMessage("2", "subtract:10:4");
        producer.sendMessage("3", "multiply:6:7");
        producer.sendMessage("4", "divide:8:2");

        producer.close();
    }
}
