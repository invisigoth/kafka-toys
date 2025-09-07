package au.com.xing.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Utility class for loading reference data from JSON files.
 */
public class ReferenceDataLoader {
    private static final ObjectMapper mapper = new ObjectMapper();

    private ReferenceDataLoader() {
        // Prevent instanciation - utility class
    }

    /**
     * Loads a JSON array from a classpath resource and converts it to a List of Strings.
     *
     * @param resourcePath the path to the JSON resource file (e.g., "reference-data/last-names.json")
     * @return a List containing the string values from the JSON array
     * @throws IOException if the resource cannot be found, read, or parsed as JSON
     * @throws IllegalArgumentException if resourcePath is null or empty
     */
    public static List<String> loadList(String resourcePath) throws IOException {
        if (null == resourcePath || resourcePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Resource path cannot be empty");
        }
        try (InputStream inputStream = ReferenceDataLoader.class.getResourceAsStream(resourcePath)) {
            if (null == inputStream) {
                throw new IOException("Resource not found: " + resourcePath);
            }
            return mapper.readValue(inputStream, new TypeReference<List<String>>() {
            });
        }
    }
}