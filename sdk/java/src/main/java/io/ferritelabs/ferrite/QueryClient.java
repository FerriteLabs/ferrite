package io.ferritelabs.ferrite;

import java.io.IOException;
import java.util.*;

/**
 * Client for Ferrite's FerriteQL query language.
 *
 * <pre>{@code
 * try (FerriteClient client = FerriteClient.connect("localhost", 6379)) {
 *     QueryClient query = new QueryClient(client);
 *     QueryResult result = query.execute("SELECT * FROM users WHERE age > 25");
 *     for (Map<String, Object> row : result.getRows()) {
 *         System.out.println(row);
 *     }
 * }
 * }</pre>
 */
public class QueryClient {

    private final FerriteClient client;

    public QueryClient(FerriteClient client) {
        this.client = client;
    }

    /**
     * Executes a FerriteQL query and returns the results.
     */
    @SuppressWarnings("unchecked")
    public QueryResult execute(String query) throws IOException {
        long start = System.nanoTime();
        Object val = client.executeRaw("QUERY", query);
        long durationNanos = System.nanoTime() - start;

        if (val == null) {
            return new QueryResult(Collections.emptyList(), 0, durationNanos);
        }

        List<Object> arr = (List<Object>) val;
        if (arr.isEmpty()) {
            return new QueryResult(Collections.emptyList(), 0, durationNanos);
        }

        // First element is column headers
        List<String> columns = new ArrayList<>();
        if (arr.get(0) instanceof List) {
            for (Object h : (List<Object>) arr.get(0)) {
                columns.add(h != null ? h.toString() : "");
            }
        }

        // Remaining elements are rows
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 1; i < arr.size(); i++) {
            if (!(arr.get(i) instanceof List)) continue;
            List<Object> rowArr = (List<Object>) arr.get(i);
            Map<String, Object> row = new LinkedHashMap<>();
            for (int j = 0; j < columns.size() && j < rowArr.size(); j++) {
                row.put(columns.get(j), parseValue(rowArr.get(j)));
            }
            rows.add(row);
        }

        return new QueryResult(rows, rows.size(), durationNanos);
    }

    private static Object parseValue(Object val) {
        if (val == null) return null;
        String s = val.toString();
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e1) {
            try {
                return Double.parseDouble(s);
            } catch (NumberFormatException e2) {
                return s;
            }
        }
    }

    /** Holds the result of a FerriteQL query. */
    public static class QueryResult {
        private final List<Map<String, Object>> rows;
        private final long count;
        private final long durationNanos;

        public QueryResult(List<Map<String, Object>> rows, long count, long durationNanos) {
            this.rows = rows;
            this.count = count;
            this.durationNanos = durationNanos;
        }

        public List<Map<String, Object>> getRows() { return rows; }
        public long getCount() { return count; }
        public long getDurationNanos() { return durationNanos; }
        public double getDurationMillis() { return durationNanos / 1_000_000.0; }
    }
}
