package io.yanwu.flink.connector.debezium;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.nio.ByteBuffer;

/**
 * 基于flink读取offset
 */
@Slf4j
public class FlinkOffsetBackingStore extends MemoryOffsetBackingStore {

    public static final String FLINK_DEBEZIUM_OFFSET_KEY = "flink.debezium.offset.key";
    public static final String FLINK_DEBEZIUM_OFFSET_VALUE = "flink.debezium.offset.value";

    @Override
    public void configure(WorkerConfig config) {
        super.configure(config);
        if (!config.originals().containsKey(FLINK_DEBEZIUM_OFFSET_KEY) || !config.originals().containsKey(FLINK_DEBEZIUM_OFFSET_VALUE)) {
            return;
        }
        data.put(ByteBuffer.wrap(config.originals().get(FLINK_DEBEZIUM_OFFSET_KEY).toString().getBytes()),
                ByteBuffer.wrap(config.originals().get(FLINK_DEBEZIUM_OFFSET_VALUE).toString().getBytes()));
    }

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    protected void save() {
        // do nothing
    }

}
