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
package com.experiment.source;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestJdbcFlagSourceConnectorConfig {
    @Test
    public void testBaseConfig() {
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
