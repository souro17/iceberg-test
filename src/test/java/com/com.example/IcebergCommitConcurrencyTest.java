import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
public class IcebergCommitConcurrencyTest {
    static IntegrationEnvironment integrationEnvironment = IntegrationEnvironment.getInstance();
    private RESTCatalog catalog;
    ExecutorService exec = Executors.newFixedThreadPool(1);

    @BeforeEach
    public void setup() {
        List.of(
                integrationEnvironment.startMinio(),
                integrationEnvironment.startNessie()
        ).forEach(IntegrationEnvironment.WaitFor::waitFor);

        var s3 = integrationEnvironment.getS3();
        String bucketName = IntegrationEnvironment.WAREHOUSE;
        boolean bucketExists = s3.listBuckets().buckets().stream()
                .anyMatch(b -> b.name().equals(bucketName));
        if (!bucketExists) {
            s3.createBucket(builder -> builder.bucket(bucketName));
        }

        String icebergUri = integrationEnvironment.getNessie().getExternalNessieUri().toString().replaceAll("api/v[12]", "iceberg/main");
        setupCatalog(icebergUri);
    }

    @Test
    public void testSchemaLostOnConcurrentCommit() throws Exception {
        String tableName = "test_table";
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get())
        );
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TableIdentifier.of("default", tableName), schema, spec);

        // Step-1: Creating new column
        table.updateSchema()
                .addColumn("data", Types.StringType.get())
                .commit();
        // Write data
        GenericRecord record1 = GenericRecord.create(table.schema());
        record1.setField("id", 1);
        record1.setField("data", "test-data");

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
        DataWriter<GenericRecord> dataWriter = Parquet.writeData(fileFactory.newOutputFile())
                .forTable(table)
                .createWriterFunc(GenericParquetWriter::create)
                .build();
        try (dataWriter) {
            dataWriter.write(record1);
        }

        // Step-2: Submit the datafile commit operation in a separate thread
        commitDataFile(table, dataWriter);

        Thread.sleep(200);

        // Step-3: Update the table with a new column
        log.info("Updating schema on original table instance");
        table.updateSchema()
                .addColumn("extra", Types.StringType.get())
                .commit();
        log.info("Updated the schema successfully");
        // Write data
        GenericRecord record2 = GenericRecord.create(table.schema());
        record2.setField("id", 2);
        record2.setField("extra", "test-extra");
        try (dataWriter) {
            dataWriter.write(record2);
        }

        // Step-4: Commit datafile
        commitDataFile(table, dataWriter);

        Thread.sleep(5000);
        Table finalTable = catalog.loadTable(TableIdentifier.of("default", tableName));
        finalTable.refresh();
        log.info("Final table schemas: {}", finalTable.schemas());
        log.info("Final table schema ID: {}", finalTable.currentSnapshot().schemaId());
        assertEquals(2, finalTable.currentSnapshot().schemaId());
    }

    private void commitDataFile(Table table, DataWriter<GenericRecord> dataWriter) {
        exec.submit(() -> {
            try {
                log.info("Committing concurrent append to stale table instance");
                table.newAppend()
                        .appendFile(dataWriter.toDataFile())
                        .commit();
                log.info("Committed concurrent append successfully");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @AfterEach
    public void teardown() {
        if (catalog != null) {
            try {
                for (TableIdentifier tableId : catalog.listTables(Namespace.of("default"))) {
                    catalog.dropTable(tableId, true);
                }
            } catch (Exception e) {
                log.warn("Failed to clean up tables in catalog", e);
            }
        }
    }

    private void setupCatalog(String icebergUri) {
        if (catalog == null) {
            Map<String, String> properties = new HashMap<>();
            properties.put("type", "rest");
            properties.put(CatalogProperties.URI, icebergUri);
            properties.put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_NONE);
            RESTCatalog newCatalog = new RESTCatalog();
            newCatalog.initialize("nessie", properties);
            catalog = newCatalog;
        }
        if (!catalog.namespaceExists(Namespace.of("default"))) {
            catalog.createNamespace(Namespace.of("default"));
        }

    }
}

