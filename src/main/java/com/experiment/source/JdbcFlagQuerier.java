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
import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

public class JdbcFlagQuerier {

    private static final Logger log = LoggerFactory.getLogger(JdbcFlagQuerier.class);

    private final DatabaseDialect dialect;
    private ResultSet resultSet;
    private Connection db;
    private final String query;
    private final TableId tableId;
    private final String flagColumn;
    private final String flagInitialStatus;
    private final ColumnId flagColumnId;
    private final long timestampDelay;
    private final ColumnId timeStampColumnId;
    private final TimeZone timeZone;
    private final TableDefinition tableDefinition;
    private final SchemaMapping schemaMapping;
    private PreparedStatement stmt;
    private final String topic;
    private final String querySuffix;
    private final int batchMaxRows;

    public JdbcFlagQuerier(
            DatabaseDialect dialect,
            String query,
            TableId tableId,
            String flagColumn,
            String flagInitialStatus,
            ColumnId flagColumnId,
            long timestampDelay,
            ColumnId timeStampColumnId,
            TimeZone timeZone,
            TableDefinition tableDefinition,
            SchemaMapping schemaMapping,
            String topic,
            String querySuffix,
            int batchMaxRows
    )  {
        this.dialect = dialect;
        this.query = query;
        this.tableId = tableId;
        this.flagColumn = flagColumn;
        this.flagInitialStatus = flagInitialStatus;
        this.flagColumnId = flagColumnId;
        this.timestampDelay = timestampDelay;
        this.timeStampColumnId = timeStampColumnId;
        this.timeZone = timeZone;
        this.tableDefinition = tableDefinition;
        this.schemaMapping = schemaMapping;
        this.topic = topic;
        this.querySuffix = querySuffix;
        this.batchMaxRows = batchMaxRows;
    }

    public PreparedStatement getOrCreateStatement(Connection db) throws SQLException {
        if(stmt != null) return stmt;
        log.debug("Creating PreparedStatement");
        ExpressionBuilder builder = dialect.expressionBuilder();
        builder.append(query);
        builder.append(" WHERE ");

        // flag column
        builder.appendList()
                .delimitedBy(", ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                .of(Collections.singletonList(flagColumnId));

        // timestamp column
        builder.append(" AND ");
        builder.appendColumnName(timeStampColumnId.name());
        builder.append(" <= ? ");

        addSuffixIfPresent(builder);
        String finalQueryString = builder.toString();
        log.debug("Prepared Sql Query: {}", finalQueryString);
        stmt = dialect.createPreparedStatement(db, finalQueryString);
        return stmt;
    }

    public void startQuery(Connection db) throws SQLException {
        this.db = db;
        stmt = getOrCreateStatement(db);
        log.trace("Sql Type for flag column: '{}' is : {}", flagColumnId.name(), tableDefinition.definitionForColumn(flagColumnId.name()).type());

        // binding flag column initial value
        dialect.bindField(stmt,
                1,
                schemaMapping.schema().field(flagColumnId.name()).schema(),
                StringToSqlType.convert(flagInitialStatus, tableDefinition.definitionForColumn(flagColumnId.name()).type()),
                tableDefinition.definitionForColumn(flagColumnId.name()));

        // binding timestamp column value
        stmt.setTimestamp(2, endTimestampValue(), DateTimeUtils.getTimeZoneCalendar(timeZone));

        log.debug("Statement to Execute: {}", stmt);
        log.debug("Setting max rows per query: {}", batchMaxRows);
        stmt.setMaxRows(batchMaxRows);
        log.info("Executing query");
        resultSet = stmt.executeQuery();
        // validating supported timestamp column type for sqlserver
        dialect.validateSpecificColumnTypes(resultSet.getMetaData(), Collections.singletonList(timeStampColumnId));
    }

    public List<SourceRecord> extractRecords() throws SQLException {
        log.trace("Extracting Records from ResultSet");
        List<SourceRecord> records = new ArrayList<>();
        while(resultSet.next()) {
            records.add(extractRecord());
        }
        return records;
    }

    public SourceRecord extractRecord() {
        Struct record = new Struct(schemaMapping.schema());
        for(SchemaMapping.FieldSetter setter: schemaMapping.fieldSetters()) {
            try {
                setter.setField(record, resultSet);
            } catch (SQLException e) {
                log.warn("SQL error mapping fields into Connect record", e);
                throw new DataException(e);
            } catch (IOException e) {
                log.warn("Error mapping fields into Connect record", e);
                throw new ConnectException(e);
            }
        }
        return new SourceRecord(null, null, topic, record.schema(), record);
    }

    public Timestamp endTimestampValue() throws SQLException {
        final long currentDbTime = dialect.currentTimeOnDB(
                stmt.getConnection(),
                DateTimeUtils.getTimeZoneCalendar(timeZone)
        ).getTime();
        return new Timestamp(currentDbTime - timestampDelay);
    }

    public void reset() {
        closeResultSetQuietly();
        closeStatementQuietly();
        releaseLocksQuietly();
    }


    private void releaseLocksQuietly() {
        if (db != null) {
            try {
                db.commit();
            } catch (SQLException e) {
                log.warn("Error while committing read transaction", e);
            }
        }
        db = null;
    }

    private void closeStatementQuietly() {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ignored) {
                // intentionally ignored
            }
        }
        stmt = null;
    }

    private void closeResultSetQuietly() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException ignored) {
                // intentionally ignored
            }
        }
        resultSet = null;
    }
    protected void addSuffixIfPresent(ExpressionBuilder builder) {
        if(!this.querySuffix.isEmpty()) {
            builder.append(" ").append(querySuffix);
        }
    }

    @Override
    public String toString() {
        return "JdbcFlagQuerier:{"
                + "table=" + tableId
                + ", query='" + query+ "'"
                + ", topicPrefix='" + topic +"'"
                + ", flagColumn='" + flagColumn +"'}";
    }
}
