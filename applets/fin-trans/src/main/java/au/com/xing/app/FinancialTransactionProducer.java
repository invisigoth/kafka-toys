import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.type.TypeReference;
import org.yaml.snakeyaml.Yaml;

import au.com.xing.util.ReferenceDataLoader;
import au.com.xing.generator.TransactionGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.apache.commons.math3.random.MersenneTwister; // prefer MT19937 random
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class FinancialTransactionProducer {

    private final Producer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final MersenneTwister random;
    private final TransactionGenerator transactionGenerator;

    // Configs
    private final String topicName;
    private final int transactionsPerSecond;

    // Default fallback values
    private static final int TRANSACTIONS_PER_SECOND = 100;

    private Properties loadConfiguration() throws IOException {
        Yaml yaml = new Yaml();
        Properties props = new Properties();

        // Try to load from classpath first (../resources)
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.yml")) {
            if (input != null) {
                Map<String, Object> yamlProps = yaml.load(input);
                convertYamlToProperties(yamlProps, "", props);
                System.out.println("Config loaded from classpath: application.yml");
                return props;
            }
        }
        throw new IOException("Unabled to load config file.");
    }

    @SuppressWarnings("unchecked")
    private void convertYamlToProperties(Map<String, Object> yamlMap, String prefix, Properties properties) {
        for (Map.Entry<String, Object> entry : yamlMap.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                convertYamlToProperties((Map<String, Object>) value, key, properties);
            } else {
                properties.setProperty(key, value.toString());
            }
        }
    }

    public FinancialTransactionProducer() throws IOException {
        this.objectMapper = new ObjectMapper();
        this.random = new MersenneTwister(System.currentTimeMillis());
        this.transactionGenerator = new TransactionGenerator();

        // Load config from YAML, and init Producer with the props
        Properties configProps = loadConfiguration();

        // Store config values as instance variables
        this.topicName = configProps.getProperty("kafka.topic.name");
        this.transactionsPerSecond = Integer.parseInt(
                configProps.getProperty("producer.transactions.per.second", String.valueOf(TRANSACTIONS_PER_SECOND)));

        // Kafka props
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

        String topicName = configProps.getProperty("kafka.topic.name");
        int transactionsPerSecond = Integer.parseInt(
                configProps.getProperty("producer.transactions.per.second", String.valueOf(TRANSACTIONS_PER_SECOND)));

        this.producer = new KafkaProducer<>(kafkaProps);

        // Print loaded configuration for verification
        System.out.println("Kafka Configuration loaded:");
        System.out.println("Bootstrap Servers: " + kafkaProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Topic: " + topicName);
        System.out.println("Transactions per second: " + transactionsPerSecond);

    }

    private ObjectNode createTransaction(String accountNumber) {
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

        transaction.put("channel", random.nextDouble() < 0.7 ? "ONLINE" : "BRANCH");

        return transaction;
    }

    public void startProducing() {
        AtomicInteger transactionCount = new AtomicInteger(0);

        System.out.println("Starting financial transaction producer...");
        System.out.println("Press Ctrl+C to stop...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down producer...");
            producer.close();
            System.out.println("Producer closed. Total transactions sent: " + transactionCount.get());
        }));

        try {
            while (true) {
                String accountNumber;
                accountNumber = transactionGenerator.generateAccountNumber();

                ObjectNode transaction = createTransaction(accountNumber);
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        this.topicName,
                        transaction.get("transactionId").asText(),
                        transaction.toString()
                );

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending transaction: " + exception.getMessage());
                    }
                });

                int currentCount = transactionCount.incrementAndGet();

                if (currentCount % 1000 == 0) {
                    System.out.printf("%,d transactions sent to pipeline\n", currentCount);
                }

                Thread.sleep(1000 / this.transactionsPerSecond);
            }
        } catch (Exception e) {
            System.err.println("Error in producer: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            FinancialTransactionProducer producer = new FinancialTransactionProducer();
            producer.startProducing();
        } catch (IOException e) {
            System.err.println("Failed to init producer: " + e.getMessage());
            e.printStackTrace();
            System.exit(1); // Exit with error code
        }
    }
}