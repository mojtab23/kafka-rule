package ir.sahab.kafkarule;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@EmbeddedKafka(partitions = 1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SpringEmbeddedKafkaTest {
    private static final Logger log = LoggerFactory.getLogger(SpringEmbeddedKafkaTest.class);

    private static final String TOPIC_NAME = "test-topic";

    private static EmbeddedKafkaBroker embeddedKafkaBroker;

    @BeforeAll
    static void setUp(EmbeddedKafkaBroker embeddedKafkaBroker) {
        SpringEmbeddedKafkaTest.embeddedKafkaBroker = embeddedKafkaBroker;
    }

    private static KafkaProducer<byte[], byte[]> buildAProducer() {
        final Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        return new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static KafkaConsumer<byte[], byte[]> buildAConsumer() {
        final Map<String, Object> props = KafkaTestUtils.consumerProps("test-group-id", "false", embeddedKafkaBroker);
        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test
    void testKafkaRuleWithSelfManagedZkServer() {
        checkTopicIsClear();

        final KafkaProducer<byte[], byte[]> kafkaProducer = buildAProducer();
        kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, "key".getBytes(), "value".getBytes()));
        kafkaProducer.close();

        final KafkaConsumer<byte[], byte[]> kafkaConsumer = buildAConsumer();
        kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<byte[], byte[]> record : records) {
            assertArrayEquals("key".getBytes(), record.key());
            assertArrayEquals("value".getBytes(), record.value());
        }
        kafkaConsumer.close();

        makeTopicDirty();
    }

    @Test
    void testDefaultProducerAndConsumer() {
        checkTopicIsClear();

        final String prefixKey = "test-key-";
        final String prefixValue = "test-value-";
        final int numRecords = 1000;

        KafkaProducer<byte[], byte[]> kafkaProducer = buildAProducer();
        for (int i = 0; i < numRecords; i++) {
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(TOPIC_NAME, (prefixKey + i).getBytes(),
                            (prefixValue + i).getBytes());
            kafkaProducer.send(record);
        }
        kafkaProducer.close();

        KafkaConsumer<byte[], byte[]> kafkaConsumer = buildAConsumer();
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        int count = 0;
        while (count < numRecords) {
            ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                assertArrayEquals((prefixKey + count).getBytes(), record.key());
                assertArrayEquals((prefixValue + count).getBytes(), record.value());
                count++;
            }
        }
        kafkaConsumer.close();

        makeTopicDirty();
    }

    @Test
    void testCustomProducerAndConsumer() {
        checkTopicIsClear();

        final String prefixKey = "test-key-";
        final String prefixValue = "test-value-";
        final int numRecords = 1000;

        final Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
        for (int i = 0; i < numRecords; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, prefixKey + i, prefixValue + i);
            kafkaProducer.send(record);
        }
        kafkaProducer.close();

        final Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group-id", "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer());
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        int count = 0;
        while (count < numRecords) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                assertEquals(prefixKey + count, record.key());
                assertEquals(prefixValue + count, record.value());
                count++;
            }
        }
        kafkaConsumer.close();

        makeTopicDirty();
    }

    private void checkTopicIsClear() {
        final KafkaConsumer<byte[], byte[]> kafkaConsumer = buildAConsumer();
        kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(1000));
        assertTrue(records.isEmpty());
        kafkaConsumer.close();
    }

    private void makeTopicDirty() {
        final KafkaProducer<byte[], byte[]> kafkaProducer = buildAProducer();
        kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, "key".getBytes(), "value".getBytes()));
        kafkaProducer.close();
    }
}
