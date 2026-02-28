package ferrite

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

// VectorResult represents a single result from a vector search.
type VectorResult struct {
	ID       string
	Distance float64
	Metadata map[string]interface{}
}

// VectorCreate creates a new vector index.
func (c *Client) VectorCreate(ctx context.Context, index string, dim int, metric string) error {
	_, err := c.do(ctx, "VECTOR.INDEX.CREATE", index, strconv.Itoa(dim), metric)
	return err
}

// VectorAdd adds a vector with optional metadata to an index.
func (c *Client) VectorAdd(ctx context.Context, index, id string, vector []float32, metadata map[string]interface{}) error {
	args := []string{"VECTOR.ADD", index, id}

	// Serialize vector as comma-separated floats
	vecStr := ""
	for i, v := range vector {
		if i > 0 {
			vecStr += ","
		}
		vecStr += strconv.FormatFloat(float64(v), 'f', -1, 32)
	}
	args = append(args, vecStr)

	// Serialize metadata as JSON if provided
	if len(metadata) > 0 {
		metaBytes, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("ferrite: marshal metadata: %w", err)
		}
		args = append(args, string(metaBytes))
	}

	_, err := c.do(ctx, args...)
	return err
}

// VectorSearch performs a nearest-neighbor search on a vector index.
func (c *Client) VectorSearch(ctx context.Context, index string, query []float32, topK int) ([]VectorResult, error) {
	// Serialize query vector
	vecStr := ""
	for i, v := range query {
		if i > 0 {
			vecStr += ","
		}
		vecStr += strconv.FormatFloat(float64(v), 'f', -1, 32)
	}

	val, err := c.do(ctx, "VECTOR.SEARCH", index, vecStr, strconv.Itoa(topK))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	// Parse results: array of [id, distance, metadata_json, id, distance, metadata_json, ...]
	arr := val.([]interface{})
	var results []VectorResult
	for i := 0; i+2 < len(arr); i += 3 {
		r := VectorResult{
			ID: fmt.Sprintf("%v", arr[i]),
		}
		if dist, ok := arr[i+1].(string); ok {
			r.Distance, _ = strconv.ParseFloat(dist, 64)
		}
		if metaStr, ok := arr[i+2].(string); ok && metaStr != "" {
			r.Metadata = make(map[string]interface{})
			json.Unmarshal([]byte(metaStr), &r.Metadata)
		}
		results = append(results, r)
	}
	return results, nil
}

// VectorDelete removes a vector from an index.
func (c *Client) VectorDelete(ctx context.Context, index, id string) error {
	_, err := c.do(ctx, "VECTOR.DELETE", index, id)
	return err
}
