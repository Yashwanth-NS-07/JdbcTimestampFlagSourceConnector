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

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class JdbcFlagSourceTaskTest {
    JdbcFlagSourceTask task;
    private EmbeddedDerby db;
    private Map<String, String> props;
    private static final Logger log = LoggerFactory.getLogger(JdbcFlagSourceTaskTest.class);


    public void setup() throws SQLException {
        task = new JdbcFlagSourceTask();
        db = new EmbeddedDerby();
        props = new HashMap<>();
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
        props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");

        db.createTable(
                "table",
                "id", "INTEGER",
                "flag", "integer",
                "lastdate", "DATE"
        );

        String sql1 = "INSERT INTO \"table\" (\"id\", \"flag\", \"lastdate\") " +
                "VALUES (1, 0, DATE('2025-06-01'))";

        String sql2 = "INSERT INTO \"table\" (\"id\", \"flag\", \"lastdate\") " +
                "VALUES (2, 0, DATE('2025-06-02'))";

        String sql3 = "INSERT INTO \"table\" (\"id\", \"flag\", \"lastdate\") " +
                "VALUES (3, 0, DATE('2025-06-03'))";
        db.execute(sql1);
        db.execute(sql2);
        db.execute(sql3);

    }
    @Before
    public void checkRecordInDBBefore() throws SQLException {
        setup();
        Connection conn = db.getConnection();
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("Select * from \"table\"");
        while(rs.next()) {
            for(int i = 1; i <=rs.getMetaData().getColumnCount(); i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = rs.getString(i);
                System.out.print(columnValue + " " + rs.getMetaData().getColumnName(i));
            }
            System.out.println();
        }
    }

    @Test
    public void startTest() throws SQLException, InterruptedException, ExecutionException {
        props.put("flag.column.name", "flag");
        props.put("timestamp.column.name", "lastdate");
        props.put("primary.key.column.names", "id  ");
        props.put("query", "select \"id\", \"flag\", \"lastdate\" from \"table\"");
        //props.put("query", "SELECT * FROM (SELECT \"id\", \"flag\", \"lastdate\" FROM \"table\") AS sub");
        props.put("query.suffix", "AND \"lastdate\" < CURRENT_DATE ORDER BY \"lastdate\" DESC");
        props.put("flag.initial.status", "0");
        props.put("table.name.format", "table");
        props.put("flag.readback.status", "1");
        props.put("db.timezone", "Asia/Kolkata");
        props.put("poll.interval.ms", "10000");
        //props.put("numeric.mapping", "precision_only");
        //props.put("numeric.mapping", "best_fit_eager_double");
        props.put("timestamp.delay.interval.ms", "0");
        task.start(props);
        //task.querier.startQuery(task.cachedConnectionProvider.getConnection());
        CompletableFuture<List<SourceRecord>> future = CompletableFuture.supplyAsync(() -> {
            try {
                return task.poll();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        List<SourceRecord> records = future.get();
        int offset = 0;
        if(records != null)
            for(SourceRecord record: records) {
                task.commitRecord(record, new RecordMetadata(new TopicPartition("topic", 0), ++offset, 2, 8987, 9, 0));
            }
        db.execute("update \"table\" set \"lastdate\" = DATE('2025-06-03') where \"id\" = 1");
        future = CompletableFuture.supplyAsync(() -> {
            try {
                return task.poll();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(2000);
        task.stop();
        future.get();
        Connection con = db.getConnection();
        ResultSet rs = con.createStatement().executeQuery("select count(*) from \"table\" where \"flag\" = 0");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();
    }
    @After
    public void checkRecordInDBAfter() throws SQLException {
        Connection conn = db.getConnection();
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("Select * from \"table\"");
        while(rs.next()) {
            for(int i = 1; i <=rs.getMetaData().getColumnCount(); i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = rs.getString(i);
                System.out.print(columnValue + " " + rs.getMetaData().getColumnName(i));
            }
            System.out.println();
        }
    }

    public void commitRecord() {
        SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null);
        log.info("calling commit record method with metadata null");
        task.commitRecord(record, null);
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("topic", 0), -1, 2, 8987, 9, 0);
        log.info("calling commit record method with offset -1");
        task.commitRecord(record, metadata);
        RecordMetadata metadata1 = new RecordMetadata(new TopicPartition("topic", 0), 88, 2, 8987, 9, 0);
        log.info("calling commit record method with offset");
        task.commitRecord(record, metadata1);
    }
}
