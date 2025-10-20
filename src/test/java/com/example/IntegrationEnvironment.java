package com.example;

import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.awaitility.Awaitility;
import org.projectnessie.testing.nessie.NessieContainer;
import org.slf4j.Logger;
import org.slf4j.MDC;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.time.Duration;

import static com.google.common.base.Preconditions.checkState;
import static org.slf4j.LoggerFactory.getLogger;

public class IntegrationEnvironment {
    public static final String WAREHOUSE = "warehouse";
    private static final Logger log = getLogger(IntegrationEnvironment.class);
    protected static Network network = Network.newNetwork();

    private static final IntegrationEnvironment instance = new IntegrationEnvironment();
    private S3Client s3;

    public static IntegrationEnvironment getInstance() {
        return instance;
    }

    private IntegrationEnvironment() {
        startMinio();
        startNessie();
    }


    NessieContainer nessie = NessieContainer.builder()
            .dockerImage("ghcr.io/projectnessie/nessie")
            .dockerTag("0.105.4")
            .build()
            .createContainer()
            .withNetworkAliases("nessie")
            .withNetwork(network)
            .withEnv(ImmutableMap.<String, String>builder()
                    // Version store settings.
                    // Uses ephemeral storage, data is lost during reset.
                    .put("nessie.version.store.type", "IN_MEMORY")
                    // Object store settings.
                    // Uses MinIO as the object store.
                    .put("nessie.catalog.default-warehouse", WAREHOUSE)
                    .put("nessie.catalog.warehouses." + WAREHOUSE + ".location", "s3://" + WAREHOUSE + "/")
                    .put("nessie.catalog.service.s3.default-options.region", "us-east-1")
                    .put("nessie.catalog.service.s3.default-options.access-key", "urn:nessie-secret:quarkus:nessie.catalog.secrets.access-key")
                    .put("nessie.catalog.secrets.access-key.name", "minioadmin")
                    .put("nessie.catalog.secrets.access-key.secret", "minioadmin")
                    // MinIO endpoint for Nessie server
                    .put("nessie.catalog.service.s3.default-options.endpoint", "http://minio:9000/")
                    .put("nessie.catalog.service.s3.default-options.path-style-access", "true")
                    .build())
            .withStartupTimeout(Duration.ofMinutes(2));
    MinIOContainer minio = new MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2025-09-07T16-13-09Z")
            .asCompatibleSubstituteFor("minio/minio"))
            .withNetworkAliases("minio")
            .withNetwork(network)
            .withExposedPorts(9000, 9001)
            .withUserName("minioadmin")
            .withPassword("minioadmin")
            .withStartupTimeout(Duration.ofMinutes(2));


    public interface WaitFor {
        void waitFor();

    }

    private WaitFor start(String name, Runnable runnable) {
        Thread starting = Thread.ofPlatform().start(() -> {
            try {
                log.atInfo().addKeyValue("container", name).log("Starting");
                try (MDC.MDCCloseable container = MDC.putCloseable("container", name)) {
                    Failsafe.with(RetryPolicy.builder()
                            .withMaxRetries(5)
                            .withBackoff(Duration.ofMillis(100), Duration.ofSeconds(10))
                            .build()).run(runnable::run);
                }
                log.atInfo().addKeyValue("container", name).log("Started");
            } catch (Exception e) {
                log.atError().addKeyValue("container", name).log("Failed to start", e);
                throw e;
            }
        });
        return () -> {
            try {
                starting.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        };
    }

    public WaitFor startNessie() {
        return start("Nessie", () -> {
            Awaitility.await().atMost(Duration.ofMinutes(1))
                    .until(minio::isRunning);
            // MinIO endpoint for clients (on the Podman/Docker host)
            nessie.addEnv("nessie.catalog.service.s3.default-options.external-endpoint", minio.getS3URL());
            startContainer(nessie);
        });
    }

    public WaitFor startMinio() {
        return start("MinIO", () -> {
            startContainer(minio);
        });
    }

    private void validateState(GenericContainer<?> container) {
        checkState(container.isRunning(), "Container %s is not running", container.getContainerName());
    }

    private void startContainer(GenericContainer<?> container) {
        synchronized (container) {
            if (container.isRunning()) {
                return;
            }
            try {
                container.start();
                validateState(container);
            } catch (Exception e) {
                log.atError()
                        .setMessage(container.getLogs(OutputFrame.OutputType.STDERR))
                        .setCause(e)
                        .log();
                container.stop();
                throw e;
            }
        }
    }

    public MinIOContainer getMinio() {
        return minio;

    }

    public NessieContainer getNessie() {
        return nessie;
    }

    public S3Client getS3() {
        checkState(minio.isRunning());
        if (s3 == null) {
            s3 = S3Client.builder()
                    .region(Region.EU_CENTRAL_1)
                    .endpointOverride(URI.create(minio.getS3URL()))
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                            minio.getUserName(),
                            minio.getPassword())))
                    .forcePathStyle(true)
                    .build();
        }
        return s3;
    }

}

