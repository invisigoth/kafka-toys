package au.com.xing.util;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class ConfigLoader {

    private ConfigLoader() {
        // prevent instantiation
    }

    public static Properties loadConfiguration(String fileName) throws IOException {
        Yaml yaml = new Yaml();
        Properties props = new Properties();

        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
            if (null != input) {
                Map<String, Object> yamlProps = yaml.load(input);
                convertYamlToProperties(yamlProps, "", props);
                System.out.println("Config loaded from classpath: " + fileName);
                return props;
            }
        }
        throw new IOException("Unable to load config file: " + fileName);
    }

    @SuppressWarnings("unchecked")
    private static void convertYamlToProperties(Map<String, Object> yamlMap, String prefix, Properties properties) {
        for (Map.Entry<String, Object> entry : yamlMap.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                convertYamlToProperties((Map<String, Object>) value, key, properties);
            } else {
                if (null == value) {
                    System.err.println("Missing required configuration: " + key);
                    System.exit(1); // Exit with error code 1
                } else {
                    properties.setProperty(key, value.toString());
                }
            }
        }
    }
}