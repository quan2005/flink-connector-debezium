package io.yanwu.flink.connector.debezium;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * debezium进行恢复的offset
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DebeziumOffset {
    private byte[] key;

    private byte[] value;

    synchronized
    public void update(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public boolean isEmpty() {
        return Objects.isNull(this.key) || Objects.isNull(this.value);
    }

    @Override
    public String toString() {
        return "DebeziumOffset{" +
                "key=" + new String(key) +
                ", value=" + new String(value) +
                '}';
    }
}
