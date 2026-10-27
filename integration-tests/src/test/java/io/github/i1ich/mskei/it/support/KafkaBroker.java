package io.github.i1ich.mskei.it.support;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.ConfluentKafkaContainer;

/**
 * {@link ConfluentKafkaContainer} with its own start script.
 *
 * <p>Why: Testcontainers 1.20.4 starts the combined broker+controller with {@code CONTROLLER://0.0.0.0:9094} in
 * {@code listeners}. cp-kafka 7.9 (Kafka 3.9) refuses that ("advertised.listeners cannot use the nonroutable
 * meta-address 0.0.0.0": the controller listener is not advertised, so Kafka falls back to the bind address).
 * docker-compose.yml avoids it by giving the controller listener a real host name; we do the same with
 * {@code localhost}. The second, in-network listener ({@code kafka:19092}) is for Schema Registry and Connect.
 */
final class KafkaBroker extends ConfluentKafkaContainer {
    static final String NETWORK_BOOTSTRAP = "kafka:19092";

    KafkaBroker(String image) {
        super(image);
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo) {
        String hostName = containerInfo.getConfig().getHostName();
        String script = "#!/bin/bash\n"
                + "export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://" + getHost() + ":" + getMappedPort(9092)
                + ",BROKER://" + hostName + ":9093,NETWORK://" + NETWORK_BOOTSTRAP + "\n"
                + "export KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,"
                + "CONTROLLER://localhost:9094,NETWORK://0.0.0.0:19092\n"
                + "export KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="
                + "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,NETWORK:PLAINTEXT\n"
                + "/etc/confluent/docker/run \n";
        copyFileToContainer(Transferable.of(script, 0777), "/tmp/testcontainers_start.sh");
    }
}
