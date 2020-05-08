package io.yanwu.flink.connector.debezium;

import io.debezium.util.Strings;
import lombok.Getter;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Getter
public class ChangeRecord {
    private Map<String, ?> before;
    private Map<String, ?> after;
    private Map<String, ?> source;
    private String op;
    private long tsMs;

    private static final List<String> OP_TYPE_SUPPORTS = Lists.newArrayList("c", "u", "r", "d");

    public ChangeRecord(SourceRecord record) {
        if (Objects.isNull(record)) {
            return;
        }
        Object value = record.value();
        if (Objects.isNull(value) || !(value instanceof Struct)) {
            return;
        }

        Map<String, ?> map = this.structToMap((Struct) value);
        if (map.containsKey("op")) {
            this.op = (String) map.get("op");
        }
        if (map.containsKey("before")) {
            this.before = (Map<String, ?>) map.get("before");
        }
        if (map.containsKey("after")) {
            this.after = (Map<String, ?>) map.get("after");
        }
        if (map.containsKey("source")) {
            this.source = (Map<String, ?>) map.get("source");
        }
        if (map.containsKey("tsMs")) {
            this.tsMs = (Long) map.get("tsMs");
        }
    }

    protected Boolean isSupportType() {
        return !Strings.isNullOrEmpty(this.op) && OP_TYPE_SUPPORTS.contains(this.op);
    }

    private Map<String, ?> structToMap(Struct struct) {
        Map<String, Object> map = Maps.newHashMap();
        for (Field field : struct.schema().fields()) {
            if (!Objects.isNull(struct.get(field)) && struct.get(field) instanceof Struct) {
                map.put(field.name(), this.structToMap((Struct) struct.get(field)));
            } else {
                map.put(field.name(), struct.get(field));
            }
        }
        return map;
    }

    @Override
    public String toString() {
        // 自定义ums
        return "ChangeRecord{" +
                "before=" + before +
                ", after=" + after +
                ", source=" + source +
                ", op='" + op + '\'' +
                ", ts=" + tsMs +
                '}';
    }
}
