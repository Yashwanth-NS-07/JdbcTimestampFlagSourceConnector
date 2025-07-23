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

import com.experiment.util.StringToSqlType;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcFlagDbWriter {

    final static private Logger log = LoggerFactory.getLogger(JdbcFlagDbWriter.class);

    final private DatabaseDialect dialect;
    final private TableId tableId;
    private JdbcFlagBufferedRecords buffer;
    private final List<String> keyFields;
    private final Set<String> allFields;
    private final TableDefinition tableDefinition;
    private final CachedConnectionProvider cachedConnectionProvider;
    private final ColumnId flagColumnId;
    private final String flagReadBackStatus;

    public JdbcFlagDbWriter(
            JdbcFlagSourceConnectorConfig config,
            DatabaseDialect dialect,
            TableId tableId,
            List<String> keyFields,
            Set<String> fields,
            TableDefinition tableDefinition,
            ColumnId flagColumnId,
            String flagReadBackStatus
    ) {
        this.dialect = dialect;
        this.tableId = tableId;
        this.keyFields = keyFields;
        this.allFields = fields;
        this.tableDefinition = tableDefinition;
        this.flagColumnId = flagColumnId;
        this.flagReadBackStatus = flagReadBackStatus;
        this.cachedConnectionProvider = connectionProvider(
                config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG),
                config.getLong(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG)
        );
    }
    public void write(Map<SourceRecord, RecordMetadata> records) throws SQLException {
        final Connection connection = cachedConnectionProvider.getConnection();
        try {
            buffer = new JdbcFlagBufferedRecords(
                    tableId,
                    dialect,
                    connection,
                    keyFields,
                    allFields,
                    tableDefinition
            );

            for(Map.Entry<SourceRecord, RecordMetadata> record: records.entrySet()) {
                RecordMetadata metadata = record.getValue();
                SourceRecord sourceRecord = record.getKey();

                Struct value = (Struct)sourceRecord.value();
                value.put(
                        flagColumnId.name(),
                        StringToSqlType.convert(flagReadBackStatus, tableDefinition.definitionForColumn(flagColumnId.name()).type())
                );
                SinkRecord sinkRecord = new SinkRecord(
                        metadata.topic(),
                        metadata.partition(),
                        null,
                        null,
                        sourceRecord.valueSchema(),
                        value,
                        metadata.offset()
                );

                buffer.add(sinkRecord);
            }
            log.debug("Flushing records in JDBC Flag Writer for table ID: {}", tableId);
            buffer.flush();
            buffer.close();
            log.trace("Committing transaction");
            connection.commit();
        } catch(SQLException e) {
            log.error("Error during write operation. Attempting rollback.", e);
            try {
                connection.rollback();
                log.info("Successfully rolled back transaction");
            } catch (SQLException sqle) {
                log.error("Failed to rollback transaction", sqle);
                e.addSuppressed(sqle);
            }
            buffer = null;
            throw e;
        }
        buffer = null;
        log.debug("Completed write operation for {} records to the database", records.size());
    }

    private CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(this.dialect, maxConnAttempts, retryBackoff) {
            @Override
            protected void onConnect(final Connection connection) throws SQLException {
                log.info("JdbcFlagDbWriter Connected");
                connection.setAutoCommit(false);
            }
        };
    }

    public void closeQueitly() {
        cachedConnectionProvider.close();
    }
}
