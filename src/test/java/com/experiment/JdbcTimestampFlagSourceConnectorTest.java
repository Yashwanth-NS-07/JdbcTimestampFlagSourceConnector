package com.experiment;

import com.experiment.source.EmbeddedDerby;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcTimestampFlagSourceConnectorTest {

    JdbcTimestampFlagSourceConnector connector;
    private EmbeddedDerby db;
    private Map<String, String> props;

    @Before
    public void setup() {
        connector = new JdbcTimestampFlagSourceConnector();
        db = new EmbeddedDerby();
        props = new HashMap<>();
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
        props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
    }

    @Before
    public void startTest() {
        props.put("flag.column.name", "a");
        props.put("table.name.format", "schema.table");
        props.put("timestamp.column.name", "t");
        props.put("primary.key.column.names", "a,b");
        props.put("query", "select * from table");
        props.put("flag.initial.status", "N");
        props.put("flag.readback.status", "y");
        connector.start(props);
        System.out.println(connector.cachedConnectionProvider.isConnectionValid(connector.cachedConnectionProvider.getConnection(),30000));
    }

    @Test
    public void tasksConfigTest() {
        List<Map<String, String>> tasks = connector.taskConfigs(4);
        for(Map<String, String> map: tasks) {
            for(Map.Entry<String, String> entry: map.entrySet()) {
                System.out.println(entry.getKey() +" : "+entry.getValue());
            }
        }
    }
    @Test
    public void validateConfig() {
        Config config = connector.validate(props);
        for(ConfigValue value: config.configValues()) {
            System.out.println(value.value());
        }
    }
    @After
    public void stopping() {
        connector.stop();
    }
}
