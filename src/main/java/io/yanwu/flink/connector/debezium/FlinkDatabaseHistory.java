package io.yanwu.flink.connector.debezium;

import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;

import java.util.function.Consumer;

public class FlinkDatabaseHistory extends AbstractDatabaseHistory {

    public FlinkDatabaseHistory() {
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
    }

    @Override
    protected void storeRecord(HistoryRecord record) {
        // do nothing
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        // do nothing
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public String toString() {
        return "flink database history";
    }
}
