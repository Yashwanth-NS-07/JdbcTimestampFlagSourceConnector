package com.experiment;

import com.experiment.source.JdbcFlagSourceConnectorConfig;
import com.experiment.source.JdbcFlagSourceTask;
import com.experiment.util.Version;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class JdbcFlagSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(JdbcFlagSourceConnector.class);

    private Map<String, String> configProperties;
    // visible for testing purpose
    /*private*/CachedConnectionProvider cachedConnectionProvider;
    private JdbcFlagSourceConnectorConfig config;
    private DatabaseDialect dialect;

    @Override
    public void start(Map<String, String> properties) throws ConnectException {
        log.info("Starting Jdbc Flag Source Connector");
        try {
            configProperties = properties;
            config = new JdbcFlagSourceConnectorConfig(properties);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start the jdbc flag source connector due to config error", e);
        }
        final String dbUrl = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
        final int maxConnectionAttempts = config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
        final long connectionRetryBackOff = config.getLong(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);
        dialect = DatabaseDialects.findBestFor(dbUrl, config);
        cachedConnectionProvider = new CachedConnectionProvider(dialect, maxConnectionAttempts, connectionRetryBackOff);
        log.info("Cached Connection Provider created");
        log.info("Initial Connection attempt with the database");
        cachedConnectionProvider.getConnection();
        if(config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG).isEmpty()) {
            throw new ConnectException("Invalid Configuration: query config is empty");
        }
        if(config.getString(JdbcFlagSourceConnectorConfig.TABLE_NAME_FORMAT_CONFIG).isEmpty()) {
            throw new ConnectException("Invalid Configuration: "
            + JdbcFlagSourceConnectorConfig.TABLE_NAME_FORMAT_CONFIG + " config is empty");
        }
        if(config.getString(JdbcFlagSourceConnectorConfig.FLAG_COLUMN_CONFIG).isEmpty()) {
            throw new ConnectException("Invalid Configuration: "+ JdbcFlagSourceConnectorConfig.FLAG_COLUMN_CONFIG +" config is empty");
        }
        if(config.getString(JdbcFlagSourceConnectorConfig.TIMESTAMP_COLUMN_CONFIG).isEmpty()) {
            throw new ConnectException("Invalid Configuration: "+ JdbcFlagSourceConnectorConfig.TIMESTAMP_COLUMN_CONFIG+" config is empty");
        }
        if(config.getString(JdbcFlagSourceConnectorConfig.PRIMARY_KEYS_CONFIG).isEmpty()) {
            throw new ConnectException("Invalid Configuration: " + JdbcFlagSourceConnectorConfig.PRIMARY_KEYS_CONFIG + " config is empty");
        }
        if(config.getString(JdbcFlagSourceConnectorConfig.FLAG_COLUMN_INITIAL_STATUS_CONFIG).isEmpty()) {
            throw new ConnectException("Invalid Configuration: " + JdbcFlagSourceConnectorConfig.FLAG_COLUMN_INITIAL_STATUS_CONFIG + " config is empty");
        }
        if(config.getString(JdbcFlagSourceConnectorConfig.FLAG_COLUMN_READBACK_STATUS_CONFIG).isEmpty()) {
            throw new ConnectException("Invalid Configuration: " + JdbcFlagSourceConnectorConfig.FLAG_COLUMN_READBACK_STATUS_CONFIG + " config is empty");
        }
        if(config.getString(JdbcFlagSourceConnectorConfig.FLAG_COLUMN_INITIAL_STATUS_CONFIG).equalsIgnoreCase(
                config.getString(JdbcFlagSourceConnectorConfig.FLAG_COLUMN_READBACK_STATUS_CONFIG)
        )) {
            throw new ConnectException("Invalid Configuration: Both initial status and readback status for flag column should not be same");

        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JdbcFlagSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Configuring tasks");
        List<Map<String, String>> tasks = new LinkedList<>();
        tasks.add(new HashMap<>(configProperties));
        return tasks;
    }

    @Override
    public void stop() {
        log.info("Stopping the jdbc flag connector");
        cachedConnectionProvider.close();
        try {
            if(dialect != null) dialect.close();
        } catch (Throwable t) {
            log.warn("Error while closing the {} dailect: ", dialect, t);
        } finally {
            dialect = null;
        }
    }

    @Override
    public ConfigDef config() {
        return JdbcFlagSourceConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public Config validate(Map<String, String> connectorProps) {
        return super.validate(connectorProps);
    }
}
