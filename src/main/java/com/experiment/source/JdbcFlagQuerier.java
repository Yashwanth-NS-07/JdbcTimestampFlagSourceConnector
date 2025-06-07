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
import java.sql.*;
import java.util.*;

public class JdbcFlagQuerier {

    private static final Logger log = LoggerFactory.getLogger(JdbcFlagQuerier.class);

    DatabaseDialect dialect;
    ResultSet resultSet;
    Connection db;
    String query;
    TableId tableId;
    String flagColumn;
    String flagInitialStatus;
    ColumnId flagColumnId;
    ColumnId timeStampColumnId;
    TableDefinition tableDefinition;
    SchemaMapping schemaMapping;
    PreparedStatement stmt;
    String topic;
    String querySuffix;

    public JdbcFlagQuerier(
            DatabaseDialect dialect,
            String query,
            TableId tableId,
            String flagColumn,
            String flagInitialStatus,
            ColumnId flagColumnId,
            ColumnId timeStampColumnId,
            TableDefinition tableDefinition,
            SchemaMapping schemaMapping,
            String topic,
            String querySuffix
    )  {
        this.dialect = dialect;
        this.query = query;
        this.tableId = tableId;
        this.flagColumn = flagColumn;
        this.flagInitialStatus = flagInitialStatus;
        this.flagColumnId = flagColumnId;
        this.timeStampColumnId = timeStampColumnId;
        this.tableDefinition = tableDefinition;
        this.schemaMapping = schemaMapping;
        this.topic = topic;
        this.querySuffix = querySuffix;
    }

    public PreparedStatement getOrCreateStatement(Connection db) throws SQLException {
        if(stmt != null) return stmt;
        log.debug("Creating PreparedStatement");
        ExpressionBuilder builder = dialect.expressionBuilder();
        builder.append(query);
        builder.append(" WHERE ");
        builder.appendList()
                .delimitedBy(", ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                .of(Collections.singletonList(flagColumnId));

        addSuffixIfPresent(builder);
        String finalQueryString = builder.toString();
        log.debug("Prepared Sql Query: {}", finalQueryString);
        stmt = dialect.createPreparedStatement(db, finalQueryString);
        return stmt;
    }

    public void startQuery (Connection db) throws SQLException {
        this.db = db;
        stmt = getOrCreateStatement(db);
        log.trace("Sql Type for flag column: '{}' is : {}", flagColumnId.name(), tableDefinition.definitionForColumn(flagColumnId.name()).type());
        dialect.bindField(stmt,
                1,
                schemaMapping.schema().field(flagColumnId.name()).schema(),
                StringToSqlType.convert(flagInitialStatus, tableDefinition.definitionForColumn(flagColumnId.name()).type()),
                tableDefinition.definitionForColumn(flagColumnId.name()));
        log.debug("Statement to Execute: {}", stmt);
        log.info("Executing query.");
        resultSet = stmt.executeQuery();
        // validating supported timestamp column type for sqlserver
        dialect.validateSpecificColumnTypes(resultSet.getMetaData(), Collections.singletonList(timeStampColumnId));
    }

    public List<SourceRecord> extractRecords() throws SQLException {
        log.trace("Extracting Records from ResultSet.");
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
