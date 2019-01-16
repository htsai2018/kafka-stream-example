package com.hct.kafka.messaging.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

public class KeyCountStream {

    private final KeyCountStreamConfig keyCountStreamConfig;
    private final KafkaStreams kafkaStreams;

    public KeyCountStream(Properties props, KeyCountStreamConfig keyCountStreamConfig) {
        this.keyCountStreamConfig = keyCountStreamConfig;
        final StreamsBuilder builder = new StreamsBuilder();

        final StoreBuilder stateStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(keyCountStreamConfig.getStoreName()),
                        Serdes.String(), Serdes.Long())
                .withLoggingEnabled(new HashMap<>());
        builder.addStateStore(stateStore);

        final String storeName = keyCountStreamConfig.getStoreName();

        KStream<String, String> sourceStream = builder.stream(keyCountStreamConfig.getSourceTopic());
        KStream<String, String> countStream = sourceStream.transform(() -> new KeyCountTransformer(storeName), storeName);
        countStream.to(keyCountStreamConfig.getSinkTopic(), Produced.with(Serdes.String(), Serdes.String()));

        final Topology topology = builder.build();

        kafkaStreams = new KafkaStreams(topology, props);

    }

    public String state() {
        return kafkaStreams.state().name();
    }

    public void start() {
        kafkaStreams.cleanUp();
        kafkaStreams.start();
    }

    public void stop() {
        kafkaStreams.close();
    }

    public void outputStoreRecords(OutputStream outputStream) throws Exception {
        final ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(keyCountStreamConfig.getStoreName(), QueryableStoreTypes.keyValueStore());
        if (store == null) {
            throw new Exception("store not found. store:" + keyCountStreamConfig.getStoreName());
        }
        KeyValueIterator<String, Long> keyValueIterator = store.all();
        while (keyValueIterator.hasNext()) {
            KeyValue<String, Long> entry = keyValueIterator.next();
            String line = String.format("%s=%d\n", entry.key, entry.value);
            outputStream.write(line.getBytes());
        }
    }

    private static class KeyCountTransformer implements Transformer<String, String, KeyValue<String, String>> {

        private String storeName;
        private KeyValueStore<String, Long> keyCountStore;

        public KeyCountTransformer(String storeName) {
            this.storeName = storeName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext context) {
            keyCountStore = (KeyValueStore<String, Long>) context
                    .getStateStore(storeName);
        }

        @Override
        public KeyValue<String, String> transform(final String key, final String value) {
            final Optional<Long> count = Optional.ofNullable(keyCountStore.get(key));
            final Long incrementedCount = count.orElse(0L) + 1;
            keyCountStore.put(key, incrementedCount);
            return KeyValue.pair(key, incrementedCount.toString());
        }

        @Override
        public void close() {
        }

    }
}
