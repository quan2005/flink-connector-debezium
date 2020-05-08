package io.yanwu.flink.connector.debezium;

import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.DescriptorProperties.*;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Rowtime.*;
import static org.apache.flink.table.descriptors.Schema.*;

public class DebeziumStreamTableSourceFactory implements StreamTableSourceFactory<Row> {
    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> map) {
        String name = Preconditions.checkNotNull(map.get("name"), "Name must not be null.");

        // schema
        DescriptorProperties descriptorProperties = this.getValidatedProperties(map);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));

        // source
        Properties properties = new Properties();
        properties.putAll(map);
        DebeziumSource source = new DebeziumSource(properties);

        return new DebeziumStreamTableSource(name, schema, source);
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, "debezium");
        context.put(CONNECTOR_VERSION, "1.0");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> supports = Lists.newArrayList(
                "name"
                , "connector.class"
                , "database.hostname"
                , "database.port"
                , "database.user"
                , "database.password"
                , "database.server.id"
                , "database.server.name"
                , "database.whitelist"
                , "database.blacklist"
                , "table.whitelist"
                , "table.blacklist"
                , "column.blacklist"
                , "include.schema.changes"
                , "include.query"
                , "max.queue.size"
                , "max.batch.size"
                , "poll.interval.ms"
                , "connect.timeout.ms"
                , "gtid.source.includes"
                , "gtid.source.excludes"
                , "tombstones.on.delete"
                , "message.key.columns"

                , "connect.keep.alive"
                , "database.ssl.mode"
                , "binlog.buffer.size"
                , "snapshot.mode"
                , "snapshot.locking.mode"
                , "snapshot.select.statement.overrides"
                , "min.row.count.to.stream.results"
                , "heartbeat.interval.ms"
                , "heartbeat.topics.prefix"
                , "database.initial.statements"
                , "snapshot.delay.ms"
                , "snapshot.fetch.size"
                , "snapshot.lock.timeout.ms"
                , "enable.time.adjuster"
                , "source.struct.version"
                , "sanitize.field.names"
        );

        // schema
        supports.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        supports.add(SCHEMA + ".#." + SCHEMA_TYPE);
        supports.add(SCHEMA + ".#." + SCHEMA_NAME);
        supports.add(SCHEMA + ".#." + SCHEMA_FROM);
        // computed column
        supports.add(SCHEMA + ".#." + TABLE_SCHEMA_EXPR);

        // time attributes
        supports.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
        supports.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
        supports.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
        supports.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
        supports.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
        supports.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
        supports.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
        supports.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
        supports.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

        // watermark
        supports.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME);
        supports.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR);
        supports.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE);

        // format wildcard
        supports.add(FORMAT + ".*");

        return supports;
    }


    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        // allow Kafka timestamps to be used, watermarks can not be received from source
        new SchemaValidator(true, true, false)
                .validate(descriptorProperties);

        return descriptorProperties;
    }
}
