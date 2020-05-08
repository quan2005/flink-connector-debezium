package io.yanwu.flink.connector.debezium;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.junit.Test;

import java.util.Properties;

public class DebeziumSourceTest {

    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = this.getEnvironment();
        Properties mysql = this.mysql();
        DebeziumSource source = new DebeziumSource(mysql);
        DataStreamSource<ChangeRecord> stream = env.addSource(source);
        stream.filter(o -> o.getSource().get("table").equals("products")).print();

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath("file:///D:/LocalWork/temp/savepoint-58d4d5-0bd3e6a004f8"));
//        env.execute(streamGraph);
        JobClient jobClient = env.executeAsync(streamGraph);
        System.out.println(jobClient.getJobID());
        Thread.sleep(30000);
        jobClient.triggerSavepoint("file:///D:\\LocalWork\\temp");
    }

    private StreamExecutionEnvironment getEnvironment() {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(host, port,
//                "D:\\LocalWork\\Gitlab\\bigdata\\flink-connector-debezium\\target\\flink-connector-debezium-1.0.0-SNAPSHOT.jar");
//
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(60000);
//        env.disableOperatorChaining();
        return env;
    }

    private Properties mysql() {
        final Properties props = new Properties();
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("name", "mysql-engine");
        props.setProperty("database.server.id", "85744");
        props.setProperty("database.server.name", "my-connector-test");

        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "3307");
        props.setProperty("database.user", "root");
        props.setProperty("database.password", "123456");

        props.setProperty("database.whitelist", "db");
//        props.setProperty("snapshot.mode", "schema_only_recovery");
//        props.setProperty("table.whitelist", "product,orders");

        return props;
    }
}
