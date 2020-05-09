package io.yanwu.flink.connector.debezium;

import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.util.FunctionalReadWriteLock;
import org.apache.commons.compress.utils.Lists;

import java.util.List;
import java.util.function.Consumer;

@ThreadSafe
public class FlinkDatabaseHistory extends AbstractDatabaseHistory {

    // todo: 没测试过不同线程之间是否会串数据，如果串数据，就加个namespace隔离一下
    public static volatile List<HistoryRecord> historyRecords = Lists.newArrayList();

    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        if (record == null) {
            return;
        }
        lock.write(() -> {
            historyRecords.add(record);
        });

    }

    @Override
    protected synchronized void recoverRecords(Consumer<HistoryRecord> records) {
        lock.write(() -> {
            if (exists()) {
                historyRecords.iterator().forEachRemaining(records);
            }
        });
    }

    @Override
    public boolean exists() {
        return !historyRecords.isEmpty();
    }
}
