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
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JdbcFlagSourceConnectorConfig extends JdbcSourceConnectorConfig {
    private static final Logger log = LoggerFactory.getLogger(JdbcFlagSourceConnectorConfig.class);
    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public static final String FLAG_COLUMN_INITIAL_STATUS_CONFIG = "flag.initial.status";
    public static final String FLAG_COLUMN_READBACK_STATUS_CONFIG = "flag.readback.status";
    public static final String FLAG_COLUMN_CONFIG = "flag.column.name";
    public static final String TIMESTAMP_COLUMN_CONFIG = "timestamp.column.name";
    public static final String PRIMARY_KEYS_CONFIG = "primary.key.column.names";
    public static final String TABLE_NAME_FORMAT_CONFIG = "table.name.format";
    public static final String TIMESTAMP_DELAY_INTERVAL_MS = "timestamp.delay.interval.ms";
    public static final long TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT = 1000;
    public static final String MAX_ROWS_PER_QUERY = "max.rows.per.query";
    public static final int MAX_ROWS_PER_QUERY_DEFAULT = 5000; // five thousand
    public static final String MAX_RETRIES = "max.retries";
    public static final int MAX_RETRIES_DEFAULT = 10;
    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    public static final int RETRY_BACKOFF_MS_DEFAULT = 3000;

    public JdbcFlagSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }
    public static ConfigDef baseConfigDef() {
        Set<String> inhertingConfigNamesSet = inheritedConfigNames();
        ConfigDef config = new ConfigDef();
        for(Map.Entry<String, ConfigDef.ConfigKey> configKey: JdbcSourceConnectorConfig.baseConfigDef().configKeys().entrySet()) {
            if(inhertingConfigNamesSet.contains(configKey.getKey())) {
                config.define(configKey.getValue());
            }
        }
        // not using jdbcsourceconnetor's connection url because it as a dependency for table whitelist and blacklist
        config.define(
            JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.HIGH,
            "connection url for database"
        ).define(
            FLAG_COLUMN_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.HIGH,
            "Flag Column name of table"
        ).define(
            FLAG_COLUMN_INITIAL_STATUS_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.HIGH,
            "Flag column's initial value"
        ).define(
            FLAG_COLUMN_READBACK_STATUS_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.HIGH,
            "Flag column's readback value"
        ).define(
            TIMESTAMP_COLUMN_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.HIGH,
            "Timestamp column name of table"
        ).define(
            TIMESTAMP_DELAY_INTERVAL_MS,
            ConfigDef.Type.LONG,
            TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT,
            ConfigDef.Importance.HIGH,
            "Data will be fetched until current time minus the delay(in milli seconds), it allows the transaction with the earlier timestamp to complete."
        ).define(
            PRIMARY_KEYS_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.HIGH,
            "Primary keys column names for table - separate the primary keys by ,(comma)"
        ).define(
           TABLE_NAME_FORMAT_CONFIG,
           ConfigDef.Type.STRING,
           "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.HIGH,
            "Schema and table name in this format: 'schema.table'," +
                    "if the database don't have concept of schema like mysql and " +
                    "mariadb, just put the table name. Note: if you are using quote identifiers(it uses by default if not specified to not to do so)" +
                    " in that cases you need to mention exact name like uppercase in oracle and lowercase in postgres"
        ).define(
            MAX_ROWS_PER_QUERY,
            ConfigDef.Type.INT,
            MAX_ROWS_PER_QUERY_DEFAULT,
            ConfigDef.Range.between(1, 10000),
            ConfigDef.Importance.MEDIUM,
            "Maximum number of rows fetched when the query is fired"
        ).define(
            MAX_RETRIES,
            ConfigDef.Type.INT,
            MAX_RETRIES_DEFAULT,
            ConfigDef.Range.atLeast(0),
            ConfigDef.Importance.LOW,
            "Maximum number of retries if the readback fails"
        ).define(
            RETRY_BACKOFF_MS,
            ConfigDef.Type.INT,
            RETRY_BACKOFF_MS_DEFAULT,
            ConfigDef.Range.atLeast(0),
            ConfigDef.Importance.LOW,
            "Backoff time in milli seconds during readback retries"
        );

        return config;
    }

    private static Set<String> inheritedConfigNames() {
        String[] inheritingConfigNames = {"connection.user",
                "connection.password",
                "jdbc.credentials.provider.class",
                "connection.attempts",
                "connection.backoff.ms",
                "catalog.pattern", // adding because of dependency for dialect
                "schema.pattern", // adding because of dependency for dialect
                "table.types", // adding because of dependency for dialect
                "timestamp.granularity",
                "numeric.precision.mapping",
                "numeric.mapping",
                "dialect.name",
                "query",
                "quote.sql.identifiers",
                "query.suffix",
                "transaction.isolation.mode",
                "query.retry.attempts",
                "poll.interval.ms",
                "batch.max.rows",
                "topic.prefix",
                "db.timezone"};
        return new HashSet<>(Arrays.asList(inheritingConfigNames));
    }
}
