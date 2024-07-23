package nicmsaraiva.com;


import nicmsaraiva.com.integration.KafkaMessageConsumer;

public class App
{
    public static void main( String[] args )
    {
        KafkaMessageConsumer consumer = new KafkaMessageConsumer("input_topic", "output_topic");
        consumer.consumeAndProcessMessages();
    }
}
