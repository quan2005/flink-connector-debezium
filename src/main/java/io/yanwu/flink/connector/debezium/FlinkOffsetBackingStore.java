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

    public static final String OFFSET_KEY_NAME = "flink.debezium.offsetKey";
    public static final String OFFSET_VALUE_NAME = "flink.debezium.offsetValue";
    private WorkerConfig config;

    @Override
    public void configure(WorkerConfig config) {
        this.config = config;
        super.configure(config);
    }

    @Override
    public synchronized void start() {
        super.start();
        if (!config.originals().containsKey(OFFSET_KEY_NAME) || !config.originals().containsKey(OFFSET_VALUE_NAME)) {
            return;
        }
        data.put(ByteBuffer.wrap(config.originals().get(OFFSET_KEY_NAME).toString().getBytes()),
                ByteBuffer.wrap(config.originals().get(OFFSET_VALUE_NAME).toString().getBytes()));
    }

    @Override
    protected void save() {
        // to nothing
    }

}
