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

import com.experiment.util.Version;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TimestampGranularity.CONNECT_LOGICAL;

public class JdbcFlagSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(JdbcFlagSourceTask.class);

    private final Time time;
    private final AtomicBoolean inPollMethod = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private JdbcFlagSourceConnectorConfig config;
    //visible for testing purpose
    CachedConnectionProvider cachedConnectionProvider;
    private DatabaseDialect dialect;
    private TableId tableId;
    private SchemaMapping schemaMapping;
    private int maxRowsPerQuery;
    private int queryRetryAttempts ;
    private int queryRetryAttempted ;
    private long numberOfLastPolledRecords;
    //visible for testing purpose
    JdbcFlagQuerier querier;
    JdbcFlagDbWriter writer;
    private ColumnId flagColumnId;
    private ColumnId timeStampColumnId;
    private List<ColumnId> primaryKeyColumnIds;
    private List<String> readBackKeyFields;
    private Set<String> readBackFields;
    private TableDefinition tableDefinition;

    private final Map<SourceRecord, RecordMetadata> commitedRecords = new ConcurrentHashMap<>();

    public JdbcFlagSourceTask(Time time) { this.time = time; }
    public JdbcFlagSourceTask() { time = Time.SYSTEM; }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Jdbc Flag source task");
        try {
            config = new JdbcFlagSourceConnectorConfig(props);
        } catch (ConfigException ex) {
            throw new ConfigException("Couldn't start the JdbcFlagSourceTask due configuration error", ex);
        }
        // checking timestamp granularity
        if(!config.getString(JdbcSourceConnectorConfig.TIMESTAMP_GRANULARITY_CONFIG).equals(CONNECT_LOGICAL.name().toLowerCase(Locale.ROOT))) {
            throw new ConfigException("Only connect_logical is supported for timestamp.granularity");
        }

        // checking numeric.mapping property
        if(config.numericMapping() == JdbcSourceConnectorConfig.NumericMapping.BEST_FIT_EAGER_DOUBLE
        || config.numericMapping() == JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY) {
            throw new ConfigException("precision_only and best_fit_eager_double are not supported numeric.mapping");
        }

        queryRetryAttempts = config.getInt(JdbcSourceConnectorConfig.QUERY_RETRIES_CONFIG);
        final String url = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
        final int maxConnAttempts = config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
        final long retryBackoff = config.getLong(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);
        final String dialectName = config.getString(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG);
        maxRowsPerQuery = config.getInt(JdbcFlagSourceConnectorConfig.MAX_ROWS_PER_QUERY);

        // resetting everytime the task starts/restarts
        commitedRecords.clear();
        queryRetryAttempted = 0;
        numberOfLastPolledRecords = Long.MAX_VALUE;

        if(dialectName != null && !dialectName.trim().isEmpty()) {
            dialect = DatabaseDialects.create(dialectName, config);
        } else {
            log.info("Finding the database dialect that is best fit for the provided JDBC URL");
            dialect = DatabaseDialects.findBestFor(url, config);
        }
        log.info("Using JDBC dialect {}", dialect.name());

        cachedConnectionProvider = connectionProvider(maxConnAttempts, retryBackoff);
        dialect.setConnectionIsolationMode(
                cachedConnectionProvider.getConnection(),
                JdbcSourceConnectorConfig.TransactionIsolationMode.valueOf(
                        config.getString(JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG)
                )
        );
        validateColumnsExists(
                config.getString(JdbcFlagSourceConnectorConfig.FLAG_COLUMN_CONFIG),
                config.getString(JdbcFlagSourceConnectorConfig.TIMESTAMP_COLUMN_CONFIG),
                Arrays.stream(config.getString(JdbcFlagSourceConnectorConfig.PRIMARY_KEYS_CONFIG).split(",")).map(String::trim).collect(Collectors.toCollection(HashSet::new))
        );
        initwriter();
        running.set(true);
    }

    private void validateColumnsExists(String flagColumn, String timeStampColumn, Set<String> primaryKeys) {
        String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
        try {
            final Connection con = cachedConnectionProvider.getConnection();
            boolean autoCommit = con.getAutoCommit();
            try {
                con.setAutoCommit(true);
                Statement st = con.createStatement();
                st.setFetchSize(0);
                ResultSetMetaData resultSetMetaData;
                try {
                    resultSetMetaData = st.executeQuery(query).getMetaData();
                } catch(SQLException ex) {
                    throw new ConfigException(ex.getMessage(), ex);
                }

                Map<ColumnId, ColumnDefinition> defnsById = dialect.describeColumns(resultSetMetaData);
                Set<String> columnsFromQuery = defnsById.keySet().stream()
                        .map(ColumnId::name)
                        .map(String::toLowerCase)
                        .collect(Collectors.toSet());

                String catalogFromQuery = null;
                for(ColumnId columnIdkey: defnsById.keySet()) {
                    if(columnIdkey.name().equalsIgnoreCase(flagColumn)) {
                        catalogFromQuery = columnIdkey.tableId().catalogName();
                    }
                }

                String[] tableNameFormat = config.getString(JdbcFlagSourceConnectorConfig.TABLE_NAME_FORMAT_CONFIG).split("\\.");
                String table = tableNameFormat[tableNameFormat.length-1];
                String schema = tableNameFormat.length >= 2? tableNameFormat[tableNameFormat.length-2]: null;
                String catalog = con.getCatalog() == null ? catalogFromQuery: con.getCatalog();

                tableId = new TableId(catalog, schema, table);
                tableDefinition = dialect.describeTable(con, tableId);

                log.info("Validating columns exist for table: {}", tableId);
                if(
                        config.getString(JdbcFlagSourceConnectorConfig.TABLE_NAME_FORMAT_CONFIG).isEmpty()
                        || tableDefinition == null
                ) {
                    throw new ConfigException(
                            "table.name.format property is incorrect, not able to fetch the table details."
                    );
                }
                Set<String> columnsFromTable = tableDefinition.columnNames().stream().map(String::toLowerCase).collect(Collectors.toSet());

                if(!flagColumn.isEmpty()) {
                    if(!columnsFromQuery.contains(flagColumn.toLowerCase(Locale.getDefault()))
                        && !columnsFromTable.contains(flagColumn.toLowerCase(Locale.getDefault()))) {
                        throw new ConfigException(
                                "Flag Column: " + flagColumn
                                        + " does not exists in the table '" + table + "' nor in query"
                        );
                    } else if(!columnsFromQuery.contains(flagColumn.toLowerCase(Locale.getDefault()))) {
                        throw new ConfigException(
                                "Flag Column: " + flagColumn
                                        + " does not exists in query"
                        );
                    } else  if(!columnsFromTable.contains(flagColumn.toLowerCase(Locale.getDefault()))){
                        throw new ConfigException(
                                "Flag Column: " + flagColumn
                                        + " exists in query but does not exists in the table '" + table + "'"
                        );
                    }
                }
                log.trace("Flag column validated: {}", flagColumn);
                if(!timeStampColumn.isEmpty()) {
                    if(!columnsFromQuery.contains(timeStampColumn.toLowerCase(Locale.getDefault()))
                            && !columnsFromTable.contains(timeStampColumn.toLowerCase(Locale.getDefault()))) {
                        throw new ConfigException(
                                "Timestamp Column: " + timeStampColumn
                                        + " does not exists in the table '" + table + "' nor in query"
                        );
                    } else if(!columnsFromQuery.contains(timeStampColumn.toLowerCase(Locale.getDefault()))) {
                        throw new ConfigException(
                                "Timestamp Column: " + timeStampColumn
                                        + " does not exists in query"
                        );
                    } else if(!columnsFromTable.contains(timeStampColumn.toLowerCase(Locale.getDefault()))){
                        throw new ConfigException(
                                "Timestamp Column: " + timeStampColumn
                                        + " exists in query but does not exists in the table '" + table + "'"
                        );
                    }
                }
                log.trace("Timestamp Column validated: {}", timeStampColumn);
                for(String primaryKey: primaryKeys) {
                    if(!primaryKey.isEmpty()) {
                        if(!columnsFromQuery.contains(primaryKey.toLowerCase(Locale.getDefault()))
                                && !columnsFromTable.contains(primaryKey.toLowerCase(Locale.getDefault()))) {
                            throw new ConfigException(
                                    "Primary Key Column: " + primaryKey
                                            + " does not exists in the table '" + table + "' nor in query"
                            );
                        } else if(!columnsFromQuery.contains(primaryKey.toLowerCase(Locale.getDefault()))) {
                            throw new ConfigException(
                                    "Primary Key Column: " + primaryKey
                                            + " does not exists in query"
                            );
                        } else if(!columnsFromTable.contains(primaryKey.toLowerCase(Locale.getDefault()))){
                            throw new ConfigException(
                                    "Primary Key Column: " + primaryKey
                                            + " exists in query but does not exists in the table '" + table + "'"
                            );
                        }
                    }
                }
                log.trace("Primary Keys validated: {}", primaryKeys.toString());
                primaryKeyColumnIds = new ArrayList<>(primaryKeys.size());
                for(Map.Entry<ColumnId, ColumnDefinition> entry: defnsById.entrySet()) {
                    if (entry.getKey().name().equalsIgnoreCase(flagColumn)) {
                        flagColumnId = entry.getKey();
                        if(!entry.getKey().aliasOrName().equals(entry.getKey().name())) {
                            throw new ConfigException(
                                    "Alias is not supported for Flag Column, use transforms instead."
                            );
                        }
                    }
                    if(entry.getKey().name().equalsIgnoreCase(timeStampColumn)) {
                        timeStampColumnId = entry.getKey();
                        if(!entry.getKey().aliasOrName().equals(entry.getKey().name())) {
                            throw new ConfigException(
                                    "Alias is not supported for Timestamp Column, use transforms instead."
                            );
                        }
                        if(entry.getValue().scale() > 3) {
                            throw new ConnectException(
                                    "Fractional seconds precision of Timestamp '" + entry.getKey().name() + "' column should be less than or equal to 3"
                            );
                        }
                    }
                    for(String primaryKey: primaryKeys) {
                        if(primaryKey.equalsIgnoreCase(entry.getKey().name())) {
                            primaryKeyColumnIds.add(entry.getKey());
                            if(!entry.getKey().aliasOrName().equals(entry.getKey().name())) {
                                throw new ConfigException(
                                        "Alias is not supported for Primary keys, use transforms instead."
                                );
                            }
                            if(entry.getValue().type() == Types.TIMESTAMP
                            && entry.getValue().scale() > 3) {
                                throw new ConnectException(
                                        "Fractional seconds precision of Primary key '" + entry.getKey().name() + "'"
                                               + " column should be less than or equal to 3"
                                );
                            }
                        }
                    }
                }
                schemaMapping = SchemaMapping.create(schema, resultSetMetaData, dialect);
                querier = new JdbcFlagQuerier(
                        dialect,
                        query,
                        tableId,
                        flagColumn,
                        config.getString(JdbcFlagSourceConnectorConfig.FLAG_COLUMN_INITIAL_STATUS_CONFIG),
                        flagColumnId,
                        config.getLong(JdbcFlagSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS),
                        timeStampColumnId,
                        config.timeZone(),
                        tableDefinition,
                        schemaMapping,
                        config.getString(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG),
                        config.getString(JdbcSourceConnectorConfig.QUERY_SUFFIX_CONFIG),
                        config.getInt(JdbcFlagSourceConnectorConfig.MAX_ROWS_PER_QUERY)
                );
            } finally {
                con.setAutoCommit(autoCommit);
            }
        } catch (SQLException e) {
            throw new ConnectException("Failed to validate the existence of the columns that are used to give the readback: flag column: " +
                    flagColumn + ", timestamp column: "+ timeStampColumn + ", and primary keys: "+ primaryKeys.toString(), e);
        }
    }

    private CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff) {
            @Override
            protected void onConnect(final Connection con) throws SQLException {
                super.onConnect(con);
                con.setAutoCommit(false);
            }
        };
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        inPollMethod.set(true);

        // writer
        try {
            giveReadback();
        } catch (ConnectException ce) {
            log.error("Error while giving the Readback", ce);
            closeResource();
            inPollMethod.set(false);
            throw new ConnectException(ce);
        }

        // reader
        if(numberOfLastPolledRecords < maxRowsPerQuery) {
            log.debug("Sleeping in poll method because number of rows returned by last poll is less than max.rows.per.query");
            sleep();
        }
        numberOfLastPolledRecords = 0;
        while(running.get()) {

            List<SourceRecord> results;
            try {
                querier.startQuery(cachedConnectionProvider.getConnection());
                results = new ArrayList<>(querier.extractRecords());
                querier.reset();
                if (results.isEmpty()) {
                    inPollMethod.set(false);
                    return null;
                }
                numberOfLastPolledRecords = results.size();
                log.trace("Number of last Polled Records: {}", numberOfLastPolledRecords);
                inPollMethod.set(false);
                // not closing resources when returning result
                return results;
            } catch (SQLNonTransientException sqlnte) {
                log.error("Non-Transient Sql Exception while running query for table: {}", querier, sqlnte);
                closeResource();
                inPollMethod.set(false);
                throw new ConnectException(sqlnte);
            } catch (SQLException sqle) {
                log.error(
                        "SQL exception while running query for table: {}, {}."
                                + " Attempting retry {} of {} attempts.",
                        querier,
                        sqle,
                        queryRetryAttempted + 1,
                        queryRetryAttempts);
                queryRetryAttempted++;
                if(queryRetryAttempted > queryRetryAttempts) {
                    closeResource();
                    inPollMethod.set(false);
                    throw new ConnectException("Failed to query to table after retries", sqle);
                }
                // sleeping for 2 seconds before retry
                time.sleep(2000);
                continue;
                //inPollMethod.set(false);
                // not closing resource here because tracking the query retry attempts
                //return null;
            } catch (Throwable t) {
                log.error("Failed to run query for table: {}", querier, t);
                closeResource();
                inPollMethod.set(false);
                throw t;
            }
        }
        inPollMethod.set(false);
        // not closing resource because task in initiated for stop
        return null;
    }

    private void giveReadback() {
        log.debug("Giving readback for the last polled records");
        if (numberOfLastPolledRecords == Long.MAX_VALUE) return;
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            while (commitedRecords.size() < numberOfLastPolledRecords) {
                time.sleep(10);
            }
        });
        try {
            log.trace("Waiting 2 minutes maximum for brokers ack");
            future.get(120, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.warn("Thread interrupted/timed-out while waiting for brokers ack," +
                    " proceeding with the current commited records.");
        }
        log.debug("Polled record: {}, Number records received broker ack: {}", numberOfLastPolledRecords, commitedRecords.size());

        if (numberOfLastPolledRecords != (long) commitedRecords.size()) {
            log.warn("Number of Polled records {} not equals to the Number for Acks {} received from broker",
                    numberOfLastPolledRecords,
                    commitedRecords.size());
        }

        int remainingRetries = config.getInt(JdbcFlagSourceConnectorConfig.MAX_RETRIES);
        while(true) {
            try {
                writer.write(commitedRecords);
                log.info("Successfully wrote {} records.", commitedRecords.size());
                commitedRecords.clear();
                break;
            } catch (SQLException sqle) {
                log.warn("ReadBack of {} records failed, remaining retries: {}",
                        commitedRecords.size(),
                        remainingRetries,
                        sqle);
                if(remainingRetries-- > 0) {
                    writer.closeQueitly();
                    initwriter();
                    log.trace("Sleeping before retrying for readback");
                    time.sleep(config.getInt(JdbcFlagSourceConnectorConfig.RETRY_BACKOFF_MS));
                } else {
                    throw new ConnectException(sqle);
                }
            }
        }
    }

    private void initwriter() {
        log.info("Initializing Readback writer");
        log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
        readBackKeyFields = new ArrayList<>();
        for(ColumnId id: primaryKeyColumnIds) {
            readBackKeyFields.add(id.name());
        }
        readBackKeyFields.add(timeStampColumnId.name());
        readBackFields = new HashSet<>(readBackKeyFields);
        readBackFields.add(flagColumnId.name());
        writer = new JdbcFlagDbWriter(
                config,
                dialect,
                tableId,
                readBackKeyFields,
                readBackFields,
                tableDefinition,
                flagColumnId,
                config.getString(JdbcFlagSourceConnectorConfig.FLAG_COLUMN_READBACK_STATUS_CONFIG));
        log.info("Jdbc Flag readback writer initialized");
    }

    @Override
    public synchronized void stop() {
        log.info("Stopping Jdbc Flag Source Task");
        running.set(false);
        while(inPollMethod.get()) {
            log.trace("Task Thread is running in poll method waiting for it to complete");
            time.sleep(1000);
        }
        giveReadback();
        numberOfLastPolledRecords = 0;
        if(writer != null) writer.closeQueitly();
        //last
        closeResource();

    }
    // call only when stoping or error occurs
    protected void closeResource() {
        log.info("Closing Resources for JDBC Flag Source Task");
        try {
            if(cachedConnectionProvider != null) {
                cachedConnectionProvider.close(true);
            }
        } catch (Throwable t) {
            log.warn("Error while closing the connections", t);
        } finally {
            cachedConnectionProvider = null;
            try {
                if(dialect != null) {
                    dialect.close();
                }
            } catch (Throwable t) {
                log.warn("Error while closing the dialect: {}", dialect.name(), t);
            } finally {
                dialect = null;
            }
        }
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if(metadata != null && metadata.hasOffset()) {
            commitedRecords.put(record, metadata);
            log.trace("Record added to give the readback");
        } else if(metadata == null) {
            log.warn("Record metadata is null, so not using the record to give readback");
        } else {
            log.warn("Record metadata is not null but also don't have any offset means not received ack from broker, so not using the record to give readback");
        }
    }

    public void sleep() {
        long nextPoll = time.milliseconds() + config.getInt(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG);
        while(running.get()) {
            long now = time.milliseconds();
            long sleepMs = Math.min(nextPoll - now, 1000);
            if(sleepMs > 0) {
                log.trace("Waiting {} ms to poll {} next", nextPoll - now, querier.toString());
                time.sleep(sleepMs);
            } else {
                return;
            }
        }
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
