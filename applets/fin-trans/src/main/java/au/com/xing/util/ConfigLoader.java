package au.com.xing.util;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;


/**
 * Utility class for loading config props from a YAML file.
 */
public class ConfigLoader {

    private static final Logger LOGGER = Logger.getLogger(ConfigLoader.class.getName());

    private ConfigLoader() {
        // Prevent instantiation - utility class
    }

    /**
     * Loads YAML configuration file from classpath and converts to Properties
     *
     * @param fileName the name of the YAML file to load from classpath
     * @return Properties object containing flattened configuration
     * @throws IOException              if file cannot be read or parsed
     * @throws IllegalArgumentException if fileName is null or empty
     * @throws ConfigurationException   if required configuration is missing
     */
    public static Properties loadConfiguration(String fileName) throws IOException {
        return loadConfiguration(fileName, false);
    }

    /**
     * Loads YAML configuration file from classpath and converts to Properties
     *
     * @param fileName        the name of the YAML file to load from classpath
     * @param allowNullValues if true, null values are skipped; if false, null values throw exception
     * @return Properties object containing flattened configuration
     * @throws IOException              if file cannot be read or parsed
     * @throws IllegalArgumentException if fileName is null or empty
     * @throws ConfigurationException   if required configuration is missing (when allowNullValues is false)
     */
    public static Properties loadConfiguration(String fileName, boolean allowNullValues) throws IOException {
        validateFileName(fileName);

        Yaml yaml = new Yaml();
        Properties props = new Properties();

        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
            if (null == input) {
                throw new IOException("Configuration file not found in classpath: " + fileName);
            }

            Map<String, Object> yamlProps;
            try {
                yamlProps = yaml.load(input);
            } catch (Exception e) {
                throw new IOException("Failed to parse YAML file: " + fileName, e);
            }

            if (null == yamlProps) {
                LOGGER.warning("YAML file is empty: " + fileName);
                return props; // Return empty properties
            }

            convertYamlToProperties(yamlProps, "", props, allowNullValues);
            LOGGER.info("Successfully loaded configuration from classpath: " + fileName);

            return props;
        }
    }

    private static void validateFileName(String fileName) {
        if (null == fileName || fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("Configuration file name cannot be null or empty");
        }
    }

    @SuppressWarnings("unchecked")
    private static void convertYamlToProperties(Map<String, Object> yamlMap, String prefix,
                                                Properties properties, boolean allowNullValues) throws ConfigurationException {

        for (Map.Entry<String, Object> entry : yamlMap.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                convertYamlToProperties((Map<String, Object>) value, key, properties, allowNullValues);
            } else if (null == value) {
                handleNullValue(key, allowNullValues);
            } else {
                properties.setProperty(key, value.toString());
            }
        }
    }

    private static void handleNullValue(String key, boolean allowNullValues) throws ConfigurationException {
        if (allowNullValues) {
            LOGGER.warning("Skipping null configuration value for key: " + key);
        } else {
            String message = "Required configuration value is missing or null: " + key;
            LOGGER.severe(message);
            throw new ConfigurationException(message);
        }
    }

    /**
     * Custom exception
     */
    public static class ConfigurationException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public ConfigurationException(String message) {
            super(message);
        }

        public ConfigurationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}