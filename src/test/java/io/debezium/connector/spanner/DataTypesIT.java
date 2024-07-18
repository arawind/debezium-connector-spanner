/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;

public class DataTypesIT extends AbstractSpannerConnectorIT {

    private static final Database pgDatabase = Database.builder()
            .generateDatabaseId()
            .dialect(Dialect.POSTGRESQL)
            .build();
    private static final Connection pgDatabaseConnection = pgDatabase.getConnection();

    private static final String tableName = "embedded_data_types_tests_table";
    private static final String changeStreamName = "embeddedDataTypesTestChangeStream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {

        databaseConnection.createTable(tableName + "(id INT64,"
                + "  boolCol BOOL,"
                + "  int64Col INT64,"
                + "  float32Col FLOAT32,"
                + "  float64Col FLOAT64,"
                + "  timestampCol TIMESTAMP,"
                + "  dateCol DATE,"
                + "  stringCol STRING(MAX),"
                + "  bytesCol BYTES(MAX),"
                + "  numericCol NUMERIC,"
                + "  jsonCol JSON,"
                + "  arrCol ARRAY<STRING(MAX)>,"
                + ") PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStreamName, tableName);

        pgDatabaseConnection.createTable(tableName + "(id bigint,"
                + "  boolCol boolean,"
                + "  int64Col bigint,"
                + "  float32Col float4,"
                + "  float64Col float8,"
                + "  timestampCol timestamptz,"
                + "  dateCol date,"
                + "  stringCol varchar,"
                + "  bytesCol bytea,"
                + "  numericCol numeric,"
                + "  jsonCol jsonb,"
                + "  arrCol varchar[],"
                + "  PRIMARY KEY (id)"
                + ")");
        pgDatabaseConnection.createChangeStream(changeStreamName, tableName);

        System.out.println("DataTypesIT is ready...");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(tableName);
        pgDatabaseConnection.dropChangeStream(changeStreamName);
        pgDatabaseConnection.dropTable(tableName);
    }

    private static Stream<Arguments> dialectProvider() {
        return Stream.of(
                Arguments.of(database, databaseConnection, "(1, true, 42, 3.14, 2.71, '1970-01-01 00:00:00 UTC',"
                + " '1970-01-01', 'stringVal', b'bytesVal', 6.023,"
                + " JSON '\"Hello\"', ['a', 'b'])"),
                Arguments.of(pgDatabase, pgDatabaseConnection, "(2, true, 42, 3.14, 2.71, '1970-01-01 00:00:00 UTC',"
                        + " '1970-01-01', 'stringVal', bytea 'bytesVal', 6.023,"
                        + " JSONB '\"Hello\"', ARRAY['a', 'b'])"));
    }

    @ParameterizedTest
    @MethodSource("dialectProvider")
    public void shouldStreamUpdatesToKafkaWithTheCorrectType(Database db, Connection dbConnection, String valueList)
            throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.database.id", db.getDatabaseId())
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();
        dbConnection.executeUpdate("INSERT INTO " + tableName
                + "(id, boolCol, int64Col, float32Col, float64Col, timestampCol,"
                + " dateCol, stringCol, bytesCol, numericCol, jsonCol, arrCol) "
                + "VALUES " + valueList);
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
        assertThat(records).hasSize(1);

        Struct record = (Struct) (records.get(0).value());
        assertThat(record.get("op")).isEqualTo("c");
        assertThat(record.schema().field("after")).isNotNull();

        Struct values = record.getStruct("after");

        assertTrue(values.getBoolean("boolCol"));
        assertThat(values.getInt64("int64Col")).isEqualTo(42);
        // TODO: uncomment after emulator is released with the FLOAT32 support.
        // assertThat(values.getFloat32("float32Col")).isEqualTo(3.14f);
        assertThat(values.getFloat64("float64Col")).isEqualTo(2.71);
        assertThat(values.getString("timestampCol")).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(values.getString("dateCol")).isEqualTo("1970-01-01");
        assertThat(values.getString("stringCol")).isEqualTo("stringVal");
        assertThat(values.getBytes("bytesCol")).isEqualTo("bytesVal".getBytes());
        assertThat(values.getString("numericCol")).isEqualTo("6.023");
        assertThat(values.getString("jsonCol")).isEqualTo("\"Hello\"");
        assertThat(values.getArray("arrCol")).containsExactly("a", "b");

        stopConnector();
        assertConnectorNotRunning();
    }
}
