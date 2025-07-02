/*
 * Copyright 2025 Yashwanth Gowda N S
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.experiment;

import com.experiment.source.EmbeddedDerby;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class JdbcTimestampFlagSourceConnectorTest {

    private static final Logger log = LoggerFactory.getLogger(JdbcTimestampFlagSourceConnectorTest.class);
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
        assertTrue(connector.cachedConnectionProvider.isConnectionValid(connector.cachedConnectionProvider.getConnection(),30000));
    }

    @Test
    public void tasksConfigTest() {
    }

    @Test
    public void validateConfig() {
    }

    @After
    public void stopping() {
        connector.stop();
    }
}
