package io.ferritelabs.ferrite;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Client for Ferrite vector search operations.
 *
 * <pre>{@code
 * try (FerriteClient client = FerriteClient.connect("localhost", 6379)) {
 *     VectorClient vectors = new VectorClient(client);
 *     vectors.createIndex("embeddings", 384, "cosine");
 *     vectors.addVector("embeddings", "doc1", new float[]{0.1f, 0.2f}, Map.of("text", "hello"));
 *     List<VectorResult> results = vectors.search("embeddings", new float[]{0.1f, 0.2f}, 10);
 * }
 * }</pre>
 */
public class VectorClient {

    private final FerriteClient client;

    public VectorClient(FerriteClient client) {
        this.client = client;
    }

    /** Creates a new vector index with the given dimension and distance metric. */
    public void createIndex(String name, int dimension, String metric) throws IOException {
        client.executeRaw("VECTOR.INDEX.CREATE", name, String.valueOf(dimension), metric);
    }

    /** Adds a vector with optional metadata to an index. */
    public void addVector(String index, String id, float[] vector, Map<String, Object> metadata)
            throws IOException {
        List<String> args = new ArrayList<>();
        args.add("VECTOR.ADD");
        args.add(index);
        args.add(id);
        args.add(encodeVector(vector));
        if (metadata != null && !metadata.isEmpty()) {
            args.add(encodeMetadata(metadata));
        }
        client.executeRaw(args.toArray(new String[0]));
    }

    /** Searches for the nearest vectors. */
    @SuppressWarnings("unchecked")
    public List<VectorResult> search(String index, float[] query, int topK) throws IOException {
        Object val = client.executeRaw("VECTOR.SEARCH", index, encodeVector(query), String.valueOf(topK));
        if (val == null) {
            return Collections.emptyList();
        }

        List<Object> arr = (List<Object>) val;
        List<VectorResult> results = new ArrayList<>();
        for (int i = 0; i + 2 < arr.size(); i += 3) {
            String rid = arr.get(i) != null ? arr.get(i).toString() : "";
            double distance = 0;
            if (arr.get(i + 1) != null) {
                distance = Double.parseDouble(arr.get(i + 1).toString());
            }
            Map<String, Object> meta = null;
            if (arr.get(i + 2) != null) {
                meta = parseMetadata(arr.get(i + 2).toString());
            }
            results.add(new VectorResult(rid, distance, meta));
        }
        return results;
    }

    /** Deletes a vector from an index. */
    public void deleteVector(String index, String id) throws IOException {
        client.executeRaw("VECTOR.DELETE", index, id);
    }

    private static String encodeVector(float[] vector) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(vector[i]);
        }
        return sb.toString();
    }

    private static String encodeMetadata(Map<String, Object> metadata) {
        // Simple JSON encoding without external dependencies
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            if (!first) sb.append(',');
            sb.append('"').append(escapeJson(entry.getKey())).append("\":");
            Object val = entry.getValue();
            if (val instanceof String) {
                sb.append('"').append(escapeJson(val.toString())).append('"');
            } else {
                sb.append(val);
            }
            first = false;
        }
        sb.append('}');
        return sb.toString();
    }

    private static Map<String, Object> parseMetadata(String json) {
        // Minimal JSON parsing for flat key-value metadata
        Map<String, Object> map = new LinkedHashMap<>();
        if (json == null || json.isEmpty() || json.equals("{}")) {
            return map;
        }
        // Strip outer braces
        String inner = json.substring(1, json.length() - 1).trim();
        if (inner.isEmpty()) return map;

        // Split by comma (simplified, doesn't handle nested objects)
        for (String pair : inner.split(",")) {
            String[] kv = pair.split(":", 2);
            if (kv.length == 2) {
                String key = kv[0].trim().replaceAll("^\"|\"$", "");
                String val = kv[1].trim();
                if (val.startsWith("\"") && val.endsWith("\"")) {
                    map.put(key, val.substring(1, val.length() - 1));
                } else {
                    try {
                        map.put(key, Double.parseDouble(val));
                    } catch (NumberFormatException e) {
                        map.put(key, val);
                    }
                }
            }
        }
        return map;
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /** Represents a single vector search result. */
    public static class VectorResult {
        private final String id;
        private final double distance;
        private final Map<String, Object> metadata;

        public VectorResult(String id, double distance, Map<String, Object> metadata) {
            this.id = id;
            this.distance = distance;
            this.metadata = metadata;
        }

        public String getId() { return id; }
        public double getDistance() { return distance; }
        public Map<String, Object> getMetadata() { return metadata; }

        @Override
        public String toString() {
            return "VectorResult{id='" + id + "', distance=" + distance + '}';
        }
    }
}
