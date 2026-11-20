package app.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConfig {

    /**
     * Returns the KAFKA_BOOTSTRAP_SERVERS environment variable when set (e.g. in Docker),
     * otherwise the given default (for running from an IDE against localhost).
     */
    public static String bootstrapServers(String defaultValue) {
        String fromEnv = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        return (fromEnv == null || fromEnv.isBlank()) ? defaultValue : fromEnv;
    }

    public static Properties baseProducerProps(String clientId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers("localhost:9092"));
        props.put("client.id", clientId);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        // optional:
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        return props;
    }

    public static Properties baseConsumerProps(String clientId, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers("localhost:9092"));
        props.put("client.id", clientId);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "latest");
        props.put("enable.auto.commit", "true");
        return props;
    }
}
