package au.com.xing.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ReferenceDataLoader {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static List<String> loadList(String resourcePath) throws IOException {
        try (InputStream is = ReferenceDataLoader.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }
            return mapper.readValue(is, new TypeReference<List<String>>() {
            });
        }
    }
}