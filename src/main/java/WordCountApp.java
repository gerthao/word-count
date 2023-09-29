import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        var builder = new StreamsBuilder();
        /*
         *  1.) Stream from Kafka.
         *  2.) Map values to lowercase.
         *  3.) Flatmap values split by space.
         *  4.) Select key to apply a key (discard the old key).
         *  5.) Group by key before aggregation.
         *  6.) Count occurrences.
         */
        var stream = builder.<String, String>stream("word-count-input");

        var wordCounts = stream
                .mapValues(v -> v.toLowerCase())
                .flatMapValues(v -> Arrays.asList(v.split(" ")))
                .selectKey((k, v) -> v)
                .groupByKey()
                .count(Named.as("Counts"));

        wordCounts.toStream().to("word-count-output");

        var streams = new KafkaStreams(builder.build(), properties);

        streams.start();

        System.out.println(streams);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
