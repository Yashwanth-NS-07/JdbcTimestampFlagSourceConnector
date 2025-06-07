package com.experiment.source;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestJdbcFlagSourceConnectorConfig {
    @Test
    public void testBaseCofnig() {
        Map<String, String> props = new HashMap<>();
        props.put("flag.column.name", "a");
        props.put("timestamp.column.name", "t");
        props.put("primary.key.column.names", "a,b");
        props.put("connection.url", "url");
        props.put("flag.initial.status", "N");
        props.put("flag.readback.status", "Y");
        JdbcFlagSourceConnectorConfig config = new JdbcFlagSourceConnectorConfig(props);
    }
}
