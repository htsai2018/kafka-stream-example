package com.hct.kafka.messaging.stream;

import com.hct.kafka.messaging.service.MessageService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileSystemUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
public class KeyCountStreamTest {

    @ClassRule
    public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, false, "my-topic", "key-count-sink");

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private MessageService messageService;

    private KeyCountStream subject;
    private KeyCountStreamConfig keyCountStreamConfig;
    private Consumer<String, String> consumer;

    private final static String  stateDir = System.getProperty("java.io.tmpdir");

    @BeforeClass
    public static void init() {
        File streamStateDir = new File(stateDir, "key-count-streams");
        if (streamStateDir.exists()) {
            FileSystemUtils.deleteRecursively(streamStateDir);
        }
        System.setProperty("spring.kafka.bootstrap-servers",
                broker.getEmbeddedKafka().getBrokersAsString());
    }

    @Before
    public void setup() throws Exception {


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "key-count-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        keyCountStreamConfig = KeyCountStreamConfig.builder()
                .sourceTopic("my-topic")
                .sinkTopic("key-count-sink")
                .storeName("key-count-store")
                .build();

        subject = new KeyCountStream(props, keyCountStreamConfig);
    }

    @After
    public void tearDown() throws Exception {
        if (subject != null) {
            subject.stop();
        }
    }

    @Test
    public void streamState() throws Exception {
        assertThat(subject.state()).isEqualTo("CREATED");

        subject.start();
        assertThat(subject.state()).isEqualTo("RUNNING");

        subject.stop();
        assertThat(subject.state()).isEqualTo("NOT_RUNNING");
    }

    @Test
    public void passMessageToSink() throws Exception {
        subject.start();

        await().atMost(30, SECONDS).untilAsserted(() ->
                assertThat(subject.state()).isEqualTo("RUNNING")
        );

        Map<String, Object> config = kafkaProperties.buildConsumerProperties();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        ConsumerFactory<String, String> sourceConsumerFactory = new DefaultKafkaConsumerFactory<>(config);
        consumer = sourceConsumerFactory.createConsumer();
        broker.getEmbeddedKafka().consumeFromAllEmbeddedTopics(consumer);

        messageService.sendMessage(new ProducerRecord("my-topic", "key-2", "2-1"));
        messageService.sendMessage(new ProducerRecord("my-topic", "key-1", "1-1"));
        messageService.sendMessage(new ProducerRecord("my-topic", "key-1", "1-2"));

        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer);
        await().atMost(30, SECONDS).untilAsserted(() ->
                assertThat(consumerRecords.count()).isGreaterThan(0)
        );

        List<KeyValue<String, String>> sourceRecords = new ArrayList<>(3);
        List<KeyValue<String, String>> sinkRecords = new ArrayList<>(3);

        Iterator<ConsumerRecord<String, String>> consumerRecordIterator = consumerRecords.iterator();
        while(consumerRecordIterator.hasNext()) {
            ConsumerRecord<String, String> record = consumerRecordIterator.next();
            if (record.topic().equals(keyCountStreamConfig.getSourceTopic())) {
                sourceRecords.add(KeyValue.pair(record.key(), record.value()));
            }
            if (record.topic().equals(keyCountStreamConfig.getSinkTopic())) {
                sinkRecords.add(KeyValue.pair(record.key(), record.value()));
            }
        }
        assertThat(sourceRecords).hasSize(3);
        assertThat(sourceRecords.get(0).key).isEqualTo("key-2");
        assertThat(sourceRecords.get(0).value).isEqualTo("2-1");
        assertThat(sourceRecords.get(1).key).isEqualTo("key-1");
        assertThat(sourceRecords.get(1).value).isEqualTo("1-1");
        assertThat(sourceRecords.get(2).key).isEqualTo("key-1");
        assertThat(sourceRecords.get(2).value).isEqualTo("1-2");

        if (sinkRecords.isEmpty()) {
            consumerRecordIterator = KafkaTestUtils.getRecords(consumer, 5000).iterator();
            while(consumerRecordIterator.hasNext()) {
                ConsumerRecord<String, String> record = consumerRecordIterator.next();
                sinkRecords.add(KeyValue.pair(record.key(), record.value()));
            }
        }
        assertThat(sinkRecords).hasSize(3);
        assertThat(sinkRecords.get(0).key).isEqualTo("key-2");
        assertThat(sinkRecords.get(0).value).isEqualTo("1");
        assertThat(sinkRecords.get(1).key).isEqualTo("key-1");
        assertThat(sinkRecords.get(1).value).isEqualTo("1");
        assertThat(sinkRecords.get(2).key).isEqualTo("key-1");
        assertThat(sinkRecords.get(2).value).isEqualTo("2");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        subject.outputStoreRecords(outputStream);
        outputStream.close();
        String result = new String(outputStream.toByteArray());
        assertThat(result).isEqualTo("key-1=2\nkey-2=1\n");
    }
}