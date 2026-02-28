package ferrite

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// QueryResult holds the result of a FerriteQL query.
type QueryResult struct {
	Rows     []map[string]interface{}
	Count    int64
	Duration time.Duration
}

// Query executes a FerriteQL query and returns the result.
func (c *Client) Query(ctx context.Context, query string) (*QueryResult, error) {
	start := time.Now()
	val, err := c.do(ctx, "QUERY", query)
	elapsed := time.Since(start)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return &QueryResult{Duration: elapsed}, nil
	}

	result := &QueryResult{
		Duration: elapsed,
	}

	arr, ok := val.([]interface{})
	if !ok {
		return result, nil
	}

	// Parse response: first element is column names, rest are rows
	if len(arr) == 0 {
		return result, nil
	}

	// Extract column headers
	var columns []string
	if headers, ok := arr[0].([]interface{}); ok {
		for _, h := range headers {
			columns = append(columns, fmt.Sprintf("%v", h))
		}
	}

	// Extract rows
	for i := 1; i < len(arr); i++ {
		rowArr, ok := arr[i].([]interface{})
		if !ok {
			continue
		}
		row := make(map[string]interface{}, len(columns))
		for j, col := range columns {
			if j < len(rowArr) {
				row[col] = parseValue(rowArr[j])
			}
		}
		result.Rows = append(result.Rows, row)
	}
	result.Count = int64(len(result.Rows))

	return result, nil
}

func parseValue(v interface{}) interface{} {
	if s, ok := v.(string); ok {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	}
	return v
}
