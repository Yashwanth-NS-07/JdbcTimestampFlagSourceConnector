# JDBC Timestamp Flag Source Connector

A lightweight, specialized Kafka Connect source connector that extends the functionality of Confluent's JDBC Source Connector by adding flag-based change tracking capabilities for streaming data from relational databases into Apache Kafka topics.

## Overview

This connector is designed as a **thin extension layer** that works in conjunction with `kafka-connect-jdbc`. Unlike traditional timestamp-based CDC (Change Data Capture) approaches, this connector tracks changes using a dedicated flag column in your database tables, making it ideal for scenarios where:

- You need explicit control over which records are synced
- Timestamp-based tracking is unreliable
- You want to mark records as "processed" or "synced" after ingestion
- Your application logic determines when records should be captured

### How It Works

```
┌────────────────────────────────────────────────────────────┐
│                    Your Database Table                     │
│  ┌────┬──────────┬───────────────┬─────────────────────┐   │
│  │ id │   name   │  updated_at   │  is_synced (flag)   │   │
│  ├────┼──────────┼───────────────┼─────────────────────┤   │
│  │ 1  │  Alice   │  2024-11-09   │      false  ◄───┐   │   │
│  │ 2  │  Bob     │  2024-11-08   │      false  ◄───┤   │   │
│  │ 3  │  Carol   │  2024-11-07   │      true       │   │   │
│  └────┴──────────┴───────────────┴─────────────────────┘   │
└────────────────────────────────────────────────────────────┘
                            │                              
                            │  Connector reads only        
                            │  records with flag=false     
                            ▼                              
┌─────────────────────────────────────────────────────────────┐
│     JdbcTimestampFlagSourceConnector                        │
│  SELECT * FROM table WHERE is_synced = false                │
└─────────────────────────────────────────────────────────────┘
                            │
                            │  Streams to Kafka(remove is_synced in transforms if needed)
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Apache Kafka                           │
│                                                             │
│  Topic: db-users                                            │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ {"id":1, "name":"Alice", "updated_at":"2024-11-09"} │    │
│  │ {"id":2, "name":"Bob", "updated_at":"2024-11-08"}   │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                            │
                            │  Recives all the Acks
                            ▼
┌─────────────────────────────────────────────────────────────┐
│     JdbcTimestampFlagSourceConnector                        │
│    Batch updates on the table                               │
│                                                             │
│  UPDATE table SET is_synced = true WHERE id = ? and         │
│                                          updated_at = ?     │
└─────────────────────────────────────────────────────────────┘

```

### Key Concept: Thin Jar Extension

This connector is a **thin jar** (small JAR file containing only this connector's code) that:

1. **Depends on kafka-connect-jdbc** for all database dialect handling (MySQL, PostgreSQL, Oracle, SQL Server, etc.)
2. **Adds flag-based tracking** on top of the standard JDBC connector functionality
3. **Requires kafka-connect-jdbc** to be present in the classpath

Think of it as a plugin that enhances the existing JDBC connector rather than replacing it.

## Architecture & Dependencies

### Dependency Chain

```
JdbcTimestampFlagSourceConnector (your thin jar)
            │
            │ depends on
            ▼
    kafka-connect-jdbc
            │
            │ provides
            ▼
┌─────────────────────────────────┐
│  - Database Dialects            │
│  - Query Builders               │
└─────────────────────────────────┘
```

### Why This Design?

- **Maintainable**: Database dialect updates come from kafka-connect-jdbc
- **Compatible**: Works with any kafka-connect-jdbc version (with proper pom.xml configuration)
- **Flexible**: Can coexist with standard JDBC connector in same Kafka Connect cluster

## Installation

### Prerequisites

- Java 8 or higher
- Apache Kafka 2.0 or higher
- Kafka Connect 2.0 or higher
- **kafka-connect-jdbc** connector (Confluent or compatible version)
- JDBC driver for your target database

### Method 1: Deploy as Separate Plugin

This method keeps your connector separate from kafka-connect-jdbc for easier updates and debugging. But might cause problem with class loaders in old kafka versions.

#### File Structure:
```
$KAFKA_HOME/
└── plugins/
    ├── confluentinc-kafka-connect-jdbc/          # Existing JDBC connector
    │   ├── kafka-connect-jdbc-10.7.4.jar
    │   ├── common-config-10.7.4.jar
    │   ├── common-utils-10.7.4.jar
    │   └── lib/
    │       ├── mysql-connector-java-8.0.33.jar
    │       ├── postgresql-42.6.0.jar
    │       └── ... (other JDBC drivers)
    │
    └── jdbc-timestamp-flag-source/                # Your connector plugin
        ├──jdbc-timestamp-flag-source-connector-1.0.0.jar
        ├── kafka-connect-jdbc-10.7.4.jar
        ├── common-config-10.7.4.jar
        ├── common-utils-10.7.4.jar
        └── lib/
            ├── mysql-connector-java-8.0.33.jar
            ├── postgresql-42.6.0.jar
            └── ... (other JDBC drivers)    
```

### Method 2: Deploy Inside kafka-connect-jdbc (Recommended for Production)

This method places your thin JAR inside the existing kafka-connect-jdbc plugin directory, reducing classpath complexity.

#### File Structure:
```
$KAFKA_HOME/
└── plugins/
    └── confluentinc-kafka-connect-jdbc/
        ├── kafka-connect-jdbc-10.7.4.jar          # Confluent's JDBC connector
        ├── common-config-10.7.4.jar
        ├── common-utils-10.7.4.jar
        ├── jdbc-timestamp-flag-source-connector-1.0.0.jar  ◄── Your thin JAR here
        └── lib/
            ├── mysql-connector-java-8.0.33.jar
            ├── postgresql-42.6.0.jar
            └── ... (other JDBC drivers)
```

#### Installation Steps:

1. **Build your connector:**
   ```bash
   git clone https://github.com/Yashwanth-NS-07/JdbcTimestampFlagSourceConnector.git
   cd JdbcTimestampFlagSourceConnector
   
   # Edit pom.xml to match your kafka-connect-jdbc version
   # See "POM Configuration" section below
   
   mvn clean package
   
2. **Locate kafka-connect-jdbc plugin directory:**
   ```bash
   # Usually located at:
   # Confluent Platform: $CONFLUENT_HOME/share/java/kafka-connect-jdbc/
   # Apache Kafka: $KAFKA_HOME/plugins/confluentinc-kafka-connect-jdbc/
   
   JDBC_PLUGIN_DIR="/path/to/kafka-connect-jdbc"
   ```

3. **Copy your thin JAR to the JDBC plugin directory:**
   ```bash
   cp target/jdbc-timestamp-flag-source-connector-1.0.0.jar \
      $JDBC_PLUGIN_DIR/
   ```

4. **Verify the structure:**
   ```bash
   ls -l $JDBC_PLUGIN_DIR/
   # Should show:
   # kafka-connect-jdbc-10.7.4.jar
   # jdbc-timestamp-flag-source-connector-1.0.0.jar  ◄── Your JAR
   # common-config-10.7.4.jar
   # lib/
   ```

5. **Restart Kafka Connect**
6. **Verify connector is loaded:**
   ```bash
   curl http://localhost:8083/connector-plugins | jq
   
   # Look for your connector in the output:
   # {
   #   "class": "com.experiment.JdbcTimestampFlagSourceConnector",
   #   "type": "source",
   #   "version": "1.0.0"
   # }
   ```

### Method Comparison

| Aspect               | Method 1 (Separate Plugin) | Method 2 (Inside JDBC Plugin) |
|----------------------|---------------------------|-------------------------------|
| **Setup Complexity** | Medium | Simple |
| **Updates**          | Easy to update independently | Must coordinate with JDBC updates |
| **Classpath**        | Relies on Kafka Connect classloader | Direct access to JDBC classes |
| **Production Use**   | Good for multi-version testing | Preferred for stable deployments |
| **Disk Space**       | Minimal overhead | Most efficient |

## POM Configuration

Your `pom.xml` needs to specify the kafka-connect-jdbc version as a **provided** dependency (meaning it's expected to be present at runtime but not packaged in your JAR).

### Example pom.xml:

```xml
        <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-connect-jdbc</artifactId>
            <version>${kafka.connect.jdbc.version}</version>  ◄── IMPORTANT
            <scope>provided</scope>
        </dependency>
```


## Features

- **Flag-based change tracking**: Uses a dedicated boolean or integer column to track record status
- **Automatic flag updates**: Optionally updates flags after reading records
- **Leverages kafka-connect-jdbc**: Reuses all database dialect implementations (MySQL, PostgreSQL, Oracle, SQL Server, etc.)
- **Lightweight**: Thin JAR design (~50KB) that depends on existing infrastructure
- **Incremental loading**: Only processes records marked with the unprocessed flag value

## Configuration

### Basic Configuration Example

Create a connector configuration file (e.g., `jdbc-source-connector.properties`):

```properties
name=jdbc-timestamp-flag-source
connector.class=com.experiment.JdbcTimestampFlagSourceConnector

connection.url=jdbc:mysql://localhost:3306/mydb
connection.user=username
connection.password=password

topic.prefix=my-topic

#Connector will append the where conditions
#Final query looks like this.
# -> query + where is_processed = 'No' and updated_at < (current_time_in_db - timestampDelay) + query.suffix
query=select * from schema.my_table

table.name.format=schema.my_table

# separate by , for combination keys
primary.key.column.names=id

timestamp.column.name=updated_at
flag.column.name=is_processed
flag.initial.status=No
flag.readback.status=Yes

poll.interval.ms=5000
batch.max.rows=100
db.timezone=Asia/Kolkata

```
### Configuration Parameters

This connector uses **two types** of configuration parameters:

1. **Inherited Configs** - Standard parameters from kafka-connect-jdbc (connection, authentication, dialect, etc.)
2. **Connector-Specific Configs** - New parameters for flag-based tracking

---

#### Connector-Specific Configuration Parameters

These are the **new configuration parameters** introduced by this connector:

##### Core Table Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `table.name.format` | String | **Yes** | - | **Full table name in format:** `schema.table` for databases with schema concept (PostgreSQL, Oracle, SQL Server) or just `table` for MySQL/MariaDB. **Important:** If using `quote.sql.identifiers=true` (default), use exact case - uppercase for Oracle, lowercase for PostgreSQL. Examples: `public.users`, `dbo.orders`, `employees` |
| `primary.key.column.names` | String | **Yes** | - | **Comma-separated list of primary key columns.** Used for updating flag values and creating Kafka message keys. Examples: `id`, `customer_id,order_id` |

##### Flag Column Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `flag.column.name` | String | **Yes** | - | **Name of the flag column** used to track record status. This column must exist in your table and should be updateable. Examples: `is_synced`, `kafka_processed`, `export_flag` |
| `flag.initial.status` | String | **Yes** | - | **Value indicating unprocessed records.** The connector queries for records with this flag value. Can be: `0`, `1`, `false`, `true`, or any string value depending on your column type. Examples: `0` (for INT), `false` (for BOOLEAN), `'N'` (for VARCHAR) |
| `flag.readback.status` | String | **Yes** | - | **Value to set after reading records.** After successfully sending records to Kafka, the connector updates the flag to this value. Examples: `1` (for INT), `true` (for BOOLEAN), `'Y'` (for VARCHAR) |

##### Timestamp Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `timestamp.column.name` | String | **Yes** | - | **Name of the timestamp column** used in where clause during readback update
| `timestamp.delay.interval.ms` | Long | No | `1000` | **Delay in milliseconds before fetching data.** Data is fetched until `(current_time - delay)`. This allows transactions with earlier timestamps to complete before being read. Useful for handling clock skew or transaction delays. Range: 0 to Long.MAX_VALUE. Example: `5000` (5 seconds delay) |

##### Batch and Performance Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `batch.max.rows` | Integer | No | `100` | **Maximum number of rows to fetch per poll.** Controls memory usage and transaction size. Overrides the kafka-connect-jdbc default. Range: 1 to 10,000. Recommended: 100-1000 for most use cases. Higher values = better throughput but more memory |

##### Retry Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `max.retries` | Integer | No | `10` | **Maximum number of retry attempts** if flag update (readback) fails. Prevents infinite retry loops. Range: 0 or higher. Set to `0` to disable retries (fail fast). |
| `retry.backoff.ms` | Integer | No | `3000` | **Backoff time in milliseconds between retry attempts.** Exponential backoff is applied: delay = `retry.backoff.ms * (2 ^ retry_attempt)`. Range: 0 or higher. Example: With default 3000ms, retries happen at 3s, 6s, 12s, 24s, etc. |

---

#### Inherited Configuration Parameters

This connector inherits the following parameters from **kafka-connect-jdbc**. For detailed documentation, see the [Confluent JDBC Source Connector Documentation](https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html).

##### Database Connection
- `connection.url`
- `connection.user`
- `connection.password`
- `jdbc.credentials.provider.class`
- `connection.attempts`
- `connection.backoff.ms`

##### Database Dialect and Schema
- `dialect.name`
- `catalog.pattern`
- `schema.pattern`
- `table.types`
- `quote.sql.identifiers`

##### Data Type Mapping
- `timestamp.granularity`
- `numeric.precision.mapping`
- `numeric.mapping`

##### Query Configuration
- `query`
- `query.suffix`
- `query.retry.attempts`
- `transaction.isolation.mode`

##### Kafka Topic Configuration
- `topic.prefix`
- `poll.interval.ms`

##### Timezone Configuration
- `db.timezone`

📚 **Full documentation:** https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html




## ⚠️ Known Limitations

Every design has trade-offs, and here are a few to be aware of:

1. Locking during flag updates: Since the connector updates rows in batches after reading, it can hold locks briefly. This may affect other transactions touching the same rows.

2. Not ideal for high-concurrency or financial systems: In workloads where rows are frequently updated by other transactions, there’s a risk of deadlocks or contention. For strict real-time or financial systems, CDC tools like Debezium or log-based streaming might be safer.

3. Batch size tuning required: Very large batches can slow down the read-back and update phases, while very small batches increase round trips.
## Performance Tips

1. **Index tracking columns**: Add indexes to flag columns for faster queries
2. **Tune batch size**: Balance between throughput and memory usage

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

For bugs and feature requests, please create an issue on the [GitHub repository](https://github.com/Yashwanth-NS-07/JdbcTimestampFlagSourceConnector/issues).

## Acknowledgments

- Apache Kafka Connect framework
- Confluent JDBC Connector (inspiration)
- The Apache Kafka community

## Author

**Yashwanth NS**
- GitHub: [@Yashwanth-NS-07](https://github.com/Yashwanth-NS-07)

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and release notes.