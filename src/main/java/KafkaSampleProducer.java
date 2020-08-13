import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaSampleProducer {
    private static Producer<Object, Object> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "Kafka-test.jpl.nasa.gov:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put("ssl.truststore.type", "jks");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafkauser\" password=\"Germproof-schedule-2020\";");
        props.put("ssl.truststore.location","/Users/rahul/dataexchange-k8s/certs/server.jks");
        props.put("ssl.truststore.password","confluent");
        props.put("schema.registry.url", "https://schemaregistry-test.jpl.nasa.gov");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        return new KafkaProducer<Object, Object>(props);
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final Producer<Object, Object> producer = createProducer();
        long time = System.currentTimeMillis();

        //String key = "key1";
        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", "value1");

        ProducerRecord<Object, Object> record = new ProducerRecord<>("test", null, avroRecord);
        try {
            producer.send(record);
        } catch(SerializationException e) {
            // may need to do something with it
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
        // then close the producer to free its resources.
        finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String... args) throws Exception {
        if (args.length == 0) {
            runProducer(5);
        } else {
            runProducer(Integer.parseInt(args[0]));
        }
    }
}
