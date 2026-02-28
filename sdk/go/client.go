// Package ferrite provides a Go client for the Ferrite key-value store.
//
// Ferrite is a high-performance Redis-compatible database with vector search,
// semantic caching, and FerriteQL support. This SDK uses the RESP2 protocol
// and requires only the Go standard library.
package ferrite

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// Client is a Ferrite client with connection pooling.
type Client struct {
	pool *Pool
	opts *options
}

// NewClient connects to a Ferrite server and returns a new Client.
func NewClient(addr string, opts ...Option) (*Client, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	pool, err := newPool(addr, o)
	if err != nil {
		return nil, err
	}

	c := &Client{pool: pool, opts: o}

	// Authenticate if password is set
	if o.password != "" {
		if err := c.auth(o.password); err != nil {
			pool.Close()
			return nil, err
		}
	}

	// Select database if non-default
	if o.db != 0 {
		if err := c.selectDB(o.db); err != nil {
			pool.Close()
			return nil, err
		}
	}

	return c, nil
}

// Close closes the client and releases all connections.
func (c *Client) Close() error {
	return c.pool.Close()
}

func (c *Client) do(ctx context.Context, args ...string) (interface{}, error) {
	conn, err := c.pool.Get()
	if err != nil {
		return nil, err
	}
	defer c.pool.Put(conn)

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.setDeadline(deadline); err != nil {
			return nil, err
		}
	}

	var lastErr error
	for attempt := 0; attempt <= c.opts.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(c.opts.retryBackoff * time.Duration(attempt)):
			}
		}

		if err := conn.writer.WriteCommand(args...); err != nil {
			lastErr = err
			continue
		}

		val, err := conn.reader.ReadValue()
		if err != nil {
			lastErr = err
			continue
		}
		return val, nil
	}
	return nil, lastErr
}

func (c *Client) auth(password string) error {
	conn, err := c.pool.Get()
	if err != nil {
		return err
	}
	defer c.pool.Put(conn)

	if err := conn.writer.WriteCommand("AUTH", password); err != nil {
		return err
	}
	_, err = conn.reader.ReadValue()
	return err
}

func (c *Client) selectDB(db int) error {
	conn, err := c.pool.Get()
	if err != nil {
		return err
	}
	defer c.pool.Put(conn)

	if err := conn.writer.WriteCommand("SELECT", strconv.Itoa(db)); err != nil {
		return err
	}
	_, err = conn.reader.ReadValue()
	return err
}

// --- String operations ---

// Get returns the value of key. Returns empty string if the key does not exist.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	val, err := c.do(ctx, "GET", key)
	if err != nil {
		return "", err
	}
	if val == nil {
		return "", nil
	}
	return val.(string), nil
}

// Set sets key to value with an optional TTL. Pass 0 for no expiration.
func (c *Client) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	args := []string{"SET", key, value}
	if ttl > 0 {
		args = append(args, "PX", strconv.FormatInt(ttl.Milliseconds(), 10))
	}
	_, err := c.do(ctx, args...)
	return err
}

// Del deletes one or more keys and returns the number of keys removed.
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	args := append([]string{"DEL"}, keys...)
	val, err := c.do(ctx, args...)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// Exists returns the number of specified keys that exist.
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	args := append([]string{"EXISTS"}, keys...)
	val, err := c.do(ctx, args...)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// Expire sets a timeout on key. Returns true if the timeout was set.
func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	val, err := c.do(ctx, "EXPIRE", key, strconv.FormatInt(int64(ttl.Seconds()), 10))
	if err != nil {
		return false, err
	}
	return val.(int64) == 1, nil
}

// TTL returns the remaining time-to-live of a key.
func (c *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	val, err := c.do(ctx, "TTL", key)
	if err != nil {
		return 0, err
	}
	return time.Duration(val.(int64)) * time.Second, nil
}

// --- List operations ---

// LPush prepends values to a list and returns the new length.
func (c *Client) LPush(ctx context.Context, key string, values ...string) (int64, error) {
	args := append([]string{"LPUSH", key}, values...)
	val, err := c.do(ctx, args...)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// RPush appends values to a list and returns the new length.
func (c *Client) RPush(ctx context.Context, key string, values ...string) (int64, error) {
	args := append([]string{"RPUSH", key}, values...)
	val, err := c.do(ctx, args...)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// LPop removes and returns the first element of a list.
func (c *Client) LPop(ctx context.Context, key string) (string, error) {
	val, err := c.do(ctx, "LPOP", key)
	if err != nil {
		return "", err
	}
	if val == nil {
		return "", nil
	}
	return val.(string), nil
}

// RPop removes and returns the last element of a list.
func (c *Client) RPop(ctx context.Context, key string) (string, error) {
	val, err := c.do(ctx, "RPOP", key)
	if err != nil {
		return "", err
	}
	if val == nil {
		return "", nil
	}
	return val.(string), nil
}

// LRange returns elements from a list within the given range.
func (c *Client) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	val, err := c.do(ctx, "LRANGE", key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10))
	if err != nil {
		return nil, err
	}
	return toStringSlice(val), nil
}

// LLen returns the length of a list.
func (c *Client) LLen(ctx context.Context, key string) (int64, error) {
	val, err := c.do(ctx, "LLEN", key)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// --- Hash operations ---

// HSet sets field-value pairs in a hash.
func (c *Client) HSet(ctx context.Context, key string, fieldValues ...string) (int64, error) {
	args := append([]string{"HSET", key}, fieldValues...)
	val, err := c.do(ctx, args...)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// HGet returns the value of a hash field.
func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	val, err := c.do(ctx, "HGET", key, field)
	if err != nil {
		return "", err
	}
	if val == nil {
		return "", nil
	}
	return val.(string), nil
}

// HGetAll returns all fields and values of a hash.
func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	val, err := c.do(ctx, "HGETALL", key)
	if err != nil {
		return nil, err
	}
	return toStringMap(val), nil
}

// HDel deletes fields from a hash and returns the number removed.
func (c *Client) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	args := append([]string{"HDEL", key}, fields...)
	val, err := c.do(ctx, args...)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// --- Set operations ---

// SAdd adds members to a set and returns the number added.
func (c *Client) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	args := append([]string{"SADD", key}, members...)
	val, err := c.do(ctx, args...)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// SMembers returns all members of a set.
func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	val, err := c.do(ctx, "SMEMBERS", key)
	if err != nil {
		return nil, err
	}
	return toStringSlice(val), nil
}

// SRem removes members from a set and returns the number removed.
func (c *Client) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	args := append([]string{"SREM", key}, members...)
	val, err := c.do(ctx, args...)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// SCard returns the number of members in a set.
func (c *Client) SCard(ctx context.Context, key string) (int64, error) {
	val, err := c.do(ctx, "SCARD", key)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// --- Sorted Set operations ---

// ZAdd adds members with scores to a sorted set and returns the number added.
func (c *Client) ZAdd(ctx context.Context, key string, score float64, member string) (int64, error) {
	val, err := c.do(ctx, "ZADD", key, strconv.FormatFloat(score, 'f', -1, 64), member)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// ZRange returns members in a sorted set within the given rank range.
func (c *Client) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	val, err := c.do(ctx, "ZRANGE", key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10))
	if err != nil {
		return nil, err
	}
	return toStringSlice(val), nil
}

// ZScore returns the score of a member in a sorted set.
func (c *Client) ZScore(ctx context.Context, key, member string) (float64, error) {
	val, err := c.do(ctx, "ZSCORE", key, member)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}
	return strconv.ParseFloat(val.(string), 64)
}

// ZRem removes members from a sorted set and returns the number removed.
func (c *Client) ZRem(ctx context.Context, key string, members ...string) (int64, error) {
	args := append([]string{"ZREM", key}, members...)
	val, err := c.do(ctx, args...)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// --- Server operations ---

// Ping sends a PING command to verify connectivity.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.do(ctx, "PING")
	return err
}

// Info returns server information.
func (c *Client) Info(ctx context.Context) (string, error) {
	val, err := c.do(ctx, "INFO")
	if err != nil {
		return "", err
	}
	if val == nil {
		return "", nil
	}
	return val.(string), nil
}

// --- Helpers ---

func toStringSlice(val interface{}) []string {
	if val == nil {
		return nil
	}
	arr := val.([]interface{})
	result := make([]string, len(arr))
	for i, v := range arr {
		if v != nil {
			result[i] = fmt.Sprintf("%v", v)
		}
	}
	return result
}

func toStringMap(val interface{}) map[string]string {
	if val == nil {
		return nil
	}
	arr := val.([]interface{})
	result := make(map[string]string, len(arr)/2)
	for i := 0; i < len(arr)-1; i += 2 {
		key := fmt.Sprintf("%v", arr[i])
		value := fmt.Sprintf("%v", arr[i+1])
		result[key] = value
	}
	return result
}
