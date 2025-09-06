package au.com.xing.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ThreadLocalRandom;
import java.util.UUID;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import au.com.xing.util.ConfigLoader;
import au.com.xing.generator.TransactionGenerator;

public class KafkaProducerService implements AutoCloseable {

    private final ObjectMapper objectMapper;
    private final KafkaProducer<String, String> producer;
    private final String topicName;
    private final long sleepIntervalMs;
    private final TransactionGenerator transactionGenerator;

    // Default fallback values
    private static final int DEFAULT_TRANSACTIONS_PER_SECOND = 1000;

    /**
     * Constructor for KafkaProducerService
     * @param generator Transaction generator for creating sample data
     * @throws IOException if configuration cannot be loaded
     * @throws IllegalArgumentException if required configuration is missing
     */
    public KafkaProducerService(TransactionGenerator generator) throws IOException {
        if (null == generator) {
            throw new IllegalArgumentException("TransactionGenerator cannot be null");
        }

        this.objectMapper = new ObjectMapper();
        this.transactionGenerator = generator;

        // Load configuration
        Properties configProps = ConfigLoader.loadConfiguration("application.yml");

        this.topicName = configProps.getProperty("kafka.topic.name");

        // Calculate precise sleep interval
        int transactionsPerSecond = Integer.parseInt(
                configProps.getProperty("producer.transactions.per.second",
                        String.valueOf(DEFAULT_TRANSACTIONS_PER_SECOND)));

        if (transactionsPerSecond <= 0) {
            throw new IllegalArgumentException("Transactions per second must be positive");
        }

        // If the rate is set to over 1,000 per second, do not sleep (division returns 0).
        this.sleepIntervalMs = 1000 / transactionsPerSecond;

        // Configure Kafka producer properties
        Properties kafkaProps = buildKafkaProperties(configProps);
        this.producer = new KafkaProducer<>(kafkaProps);

        // Print loaded configuration for verification
        logConfiguration(kafkaProps, transactionsPerSecond);
    }

    private Properties buildKafkaProperties(Properties configProps) {
        Properties kafkaProps = new Properties();

        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                configProps.getProperty("kafka.bootstrap.servers"));
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                configProps.getProperty("kafka.key.serializer", StringSerializer.class.getName()));
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                configProps.getProperty("kafka.value.serializer", StringSerializer.class.getName()));
        kafkaProps.put(ProducerConfig.ACKS_CONFIG,
                configProps.getProperty("kafka.producer.acks", "1"));
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG,
                Integer.parseInt(configProps.getProperty("kafka.producer.retries", "3")));
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG,
                Integer.parseInt(configProps.getProperty("kafka.producer.batch.size", "16384")));
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG,
                Integer.parseInt(configProps.getProperty("kafka.producer.linger.ms", "1")));
        kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                Integer.parseInt(configProps.getProperty("kafka.producer.buffer.memory", "33554432")));

        return kafkaProps;
    }

    private void logConfiguration(Properties kafkaProps, int transactionsPerSecond) {
        System.out.println("Kafka Configuration loaded:");
        System.out.println("Bootstrap Servers: " + kafkaProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Topic: " + topicName);
        System.out.println("Transactions per second: " + transactionsPerSecond);
        System.out.println("Sleep interval (ms): " + sleepIntervalMs);
    }

    private ObjectNode createTransaction() {
        ObjectNode transaction = objectMapper.createObjectNode();

        transaction.put("transactionId", UUID.randomUUID().toString());
        transaction.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        transaction.put("fromBSB", transactionGenerator.generateBSB());
        transaction.put("fromAccountNumber", transactionGenerator.generateAccountNumber());
        transaction.put("fromAccountName", transactionGenerator.generateAccountName());

        transaction.put("toBSB", transactionGenerator.generateBSB());
        transaction.put("toAccountNumber", transactionGenerator.generateAccountNumber());
        transaction.put("toAccountName", transactionGenerator.generateAccountName());

        double amount = transactionGenerator.generateAmount();
        transaction.put("amount", Math.round(amount * 100.0) / 100.0);
        transaction.put("currency", "AUD");
        transaction.put("reference", transactionGenerator.generateTransactionReference());

        // Use ThreadLocalRandom for better thread safety
        transaction.put("channel", ThreadLocalRandom.current().nextDouble() < 0.7 ? "ONLINE" : "BRANCH");

        return transaction;
    }

    public void produce() {
        AtomicInteger transactionCount = new AtomicInteger(0);
        AtomicBoolean running = new AtomicBoolean(true);

        System.out.println("Starting financial transaction producer...");
        System.out.println("Press Ctrl+C to stop...");

        // Shutdown hook for graceful termination
        Thread shutdownHook = new Thread(() -> {
            System.out.println("\nShutdown signal received...");
            running.set(false);
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try {
            while (running.get()) {
                try {
                    ObjectNode transaction = createTransaction();
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            this.topicName,
                            transaction.get("transactionId").asText(),
                            transaction.toString()
                    );

                    producer.send(record, (metadata, exception) -> {
                        if (null != exception) {
                            System.err.println("Error sending transaction: " + exception.getMessage());
                        }
                    });

                    int currentCount = transactionCount.incrementAndGet();
                    if (0 == currentCount % 1000) {
                        System.out.printf("%,d transactions sent to pipeline%n", currentCount);
                    }

                    Thread.sleep(sleepIntervalMs);

                } catch (InterruptedException e) {
                    System.out.println("Producer interrupted, shutting down...");
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (Exception e) {
            System.err.println("Error in producer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            close();
            System.out.println("Producer closed. Total transactions sent: " + transactionCount.get());

            // Remove shutdown hook since we're already shutting down
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException e) {
                // Hook was already removed or JVM is shutting down
            }
        }
    }

    @Override
    public void close() {
        if (null != producer) {
            producer.close();
        }
    }
}