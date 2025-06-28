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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class JdbcFlagBufferedRecords {

    final static private Logger log = LoggerFactory.getLogger(JdbcFlagBufferedRecords.class);

    final private TableId tableId;
    final private DatabaseDialect dialect;
    private PreparedStatement updatePreparedStatement;
    private DatabaseDialect.StatementBinder updateStatementBinder;
    private List<SinkRecord> records = new ArrayList<>();
    private final List<String> keyFields;
    private final Set<String> fields;
    private final TableDefinition tableDefinition;
    private final Connection conn;

    public JdbcFlagBufferedRecords(
        TableId tableId,
        DatabaseDialect dialect,
        Connection conn,
        List<String> keyFields,
        Set<String> fields,
        TableDefinition tableDefinition
    ) {
        this.tableId = tableId;
        this.dialect = dialect;
        this.conn = conn;
        this.keyFields = keyFields;
        this.fields = fields;
        this.tableDefinition = tableDefinition;
    }

    public void add(SinkRecord record) throws SQLException {
        if(updateStatementBinder == null) {
            final SchemaPair schemaPair = new SchemaPair(
                    record.keySchema(),
                    record.valueSchema()
            );
            FieldsMetadata fieldsMetadata = FieldsMetadata.extract(
                    tableId.tableName(),
                    JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
                    keyFields,
                    fields,
                    new SchemaPair(
                            record.keySchema(),
                            record.valueSchema()
                    ));

            final String insertSql = dialect.buildUpdateStatement(
                    tableId,
                    asColumns(fieldsMetadata.keyFieldNames),
                    asColumns(fieldsMetadata.nonKeyFieldNames),
                    tableDefinition
            );
            updatePreparedStatement = dialect.createPreparedStatement(conn, insertSql);
            updateStatementBinder = dialect.statementBinder(
                    updatePreparedStatement,
                    JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
                    schemaPair,
                    fieldsMetadata,
                    tableDefinition, //for backward compatibility
                    JdbcSinkConfig.InsertMode.UPDATE,
                    true // for backward compatibility
            );
        }
        records.add(record);
    }

    public List<SinkRecord> flush() throws SQLException {
        if(records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        for(SinkRecord record: records) {
            updateStatementBinder.bindRecord(record);
        }
        log.debug("Flushing {} buffered records", records.size());
        // readback update
        int[] batchStatus = updatePreparedStatement.executeBatch();
        for (int updateCount : batchStatus) {
            if (updateCount == Statement.EXECUTE_FAILED) {
                throw new BatchUpdateException(
                        "Execution failed for part of the batch update", batchStatus);
            }
        }
        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        return flushedRecords;
    }

    public void close() throws SQLException {
        log.debug(
                "Closing BufferedRecords with updatePreparedStatement: {}",
                updatePreparedStatement
        );
        if (nonNull(updatePreparedStatement)) {
            updatePreparedStatement.close();
            updatePreparedStatement = null;
        }
    }

    private Collection<ColumnId> asColumns(Collection<String> names) {
        return names.stream()
                .map(name -> new ColumnId(tableId, name))
                .collect(Collectors.toList());
    }
}
