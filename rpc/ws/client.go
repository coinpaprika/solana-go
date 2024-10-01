// Copyright 2021 github.com/gagliardetto
// This file has been modified by github.com/gagliardetto
//
// Copyright 2020 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var errDead = errors.New("connection lost")

type reconnectFunc func(context.Context) (*websocket.Conn, error)

type result interface{}

type connSubscriptions struct {
	subscriptionByRequestID map[uint64]*Subscription
	subscriptionByWSSubID   map[uint64]*Subscription
}

type Client struct {
	rpcURL string
	conn   *websocket.Conn
	lock   sync.RWMutex

	connToSubscriptions map[*websocket.Conn]*connSubscriptions

	reconnectFunc reconnectFunc
	pongReceived  chan struct{}
	closed        chan struct{}
	msgChan       chan []byte
}

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second
	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second
	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10
)

// Connect creates a new websocket client connecting to the provided endpoint.
func Connect(ctx context.Context, rpcEndpoint string) (c *Client, err error) {
	return ConnectWithOptions(ctx, rpcEndpoint, nil)
}

// ConnectWithOptions creates a new websocket client connecting to the provided
// endpoint with a http header if available The http header can be helpful to
// pass basic authentication params as prescribed
// ref https://github.com/gorilla/websocket/issues/209
func ConnectWithOptions(ctx context.Context, rpcEndpoint string, opt *Options) (*Client, error) {
	c := &Client{
		rpcURL:              rpcEndpoint,
		connToSubscriptions: make(map[*websocket.Conn]*connSubscriptions),
		pongReceived:        make(chan struct{}),
		closed:              make(chan struct{}),
		msgChan:             make(chan []byte),
	}

	dialer := &websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  DefaultHandshakeTimeout,
		EnableCompression: true,
	}

	if opt != nil && opt.HandshakeTimeout > 0 {
		dialer.HandshakeTimeout = opt.HandshakeTimeout
	}

	var httpHeader http.Header = nil
	if opt != nil && opt.HttpHeader != nil && len(opt.HttpHeader) > 0 {
		httpHeader = opt.HttpHeader
	}

	connect := func(ctx context.Context) (*websocket.Conn, error) {
		header := httpHeader.Clone()
		conn, resp, err := dialer.DialContext(ctx, rpcEndpoint, header)
		if err != nil {
			if resp != nil {
				body, _ := io.ReadAll(resp.Body)
				err = fmt.Errorf("new ws client: dial: %w, status: %s, body: %q", err, resp.Status, string(body))
			} else {
				err = fmt.Errorf("new ws client: dial: %w", err)
			}
		}
		return conn, err
	}

	c.reconnectFunc = connect
	if err := c.connect(ctx); err != nil {
		return nil, err
	}
	go c.pingLoop()
	for i := 0; i < 8; i++ {
		go c.msgLoop()
	}
	return c, nil
}

func (c *Client) write(ctx context.Context, data []byte, retry bool) error {
	if retry {
		// The previous write failed. Try to establish a new connection.
		if err := c.connectNoLock(ctx); err != nil {
			return err
		}
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(writeWait)
	}
	zlog.Debug("writing data to conn", zap.String("data", string(data)))
	c.conn.SetWriteDeadline(deadline)
	err := c.conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		if !retry {
			return c.write(ctx, data, true)
		}
	}
	return err
}

func (c *Client) connect(ctx context.Context) error {
	if c.reconnectFunc == nil {
		return errDead
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	return c.connectNoLock(ctx)
}

func (c *Client) connectNoLock(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, writeWait)
		defer cancel()
	}

	conn, err := c.reconnectFunc(ctx)
	if err != nil {
		zlog.Debug("RPC client reconnect failed", zap.Error(err))
		return err
	} else {
		zlog.Debug("RPC client reconnected")
	}

	c.connToSubscriptions[conn] = &connSubscriptions{
		subscriptionByRequestID: make(map[uint64]*Subscription),
		subscriptionByWSSubID:   make(map[uint64]*Subscription),
	}
	conn.SetPongHandler(func(appData string) error {
		select {
		case c.pongReceived <- struct{}{}:
		case <-c.closed:
		}
		return nil
	})
	c.conn = conn

	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				// Close all subscriptions associated with this connection.
				// Two scenarios can happen here:
				// 1. Retry logic executed earlier, and a new connection has already been established.
				//    This means we need to clean up resources linked to the old connection.
				// 2. Retry logic has not been executed yet, so this cleanup will ensure
				//    all resources tied to this connection are properly released before retrying.
				c.closeAllSubscription(conn, err)
				return
			}
			select {
			case <-c.closed:
			case c.msgChan <- msg:
			}
		}
	}()

	return nil
}

func (wc *Client) pingLoop() {
	var pingTimer = time.NewTimer(pingPeriod)
	defer pingTimer.Stop()

	for {
		select {
		case <-wc.closed:
			return
		case <-pingTimer.C:
			wc.lock.Lock()
			wc.conn.SetWriteDeadline(time.Now().Add(writeWait))
			wc.conn.WriteMessage(websocket.PingMessage, nil)
			wc.conn.SetReadDeadline(time.Now().Add(pongWait))
			wc.lock.Unlock()
			pingTimer.Reset(30 * time.Second)
		case <-wc.pongReceived:
			wc.conn.SetReadDeadline(time.Time{})
		}
	}
}

func (c *Client) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	close(c.closed)
	c.conn.Close()
}

// TODO remove
func (c *Client) Close2() {
	c.conn.Close()
}

func (c *Client) msgLoop() {
	for {
		select {
		case <-c.closed:
			return
		case message := <-c.msgChan:
			c.handleMessage(message)
		}
	}
}

// GetUint64 returns the value retrieved by `Get`, cast to a uint64 if possible.
// If key data type do not match, it will return an error.
func getUint64(data []byte, keys ...string) (val uint64, err error) {
	v, t, _, e := jsonparser.Get(data, keys...)
	if e != nil {
		return 0, e
	}
	if t != jsonparser.Number {
		return 0, fmt.Errorf("Value is not a number: %s", string(v))
	}
	return strconv.ParseUint(string(v), 10, 64)
}

func getUint64WithOk(data []byte, path ...string) (uint64, bool) {
	val, err := getUint64(data, path...)
	if err == nil {
		return val, true
	}
	return 0, false
}

func (c *Client) handleMessage(message []byte) {
	// when receiving message with id. the result will be a subscription number.
	// that number will be associated to all future message destine to this request

	requestID, ok := getUint64WithOk(message, "id")
	if ok {
		subID, _ := getUint64WithOk(message, "result")
		c.handleNewSubscriptionMessage(requestID, subID)
		return
	}

	subID, _ := getUint64WithOk(message, "params", "subscription")
	c.handleSubscriptionMessage(subID, message)
}

func (c *Client) handleNewSubscriptionMessage(requestID, subID uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if traceEnabled {
		zlog.Debug("received new subscription message",
			zap.Uint64("message_id", requestID),
			zap.Uint64("subscription_id", subID),
		)
	}

	subscriptions, ok := c.connToSubscriptions[c.conn]
	if !ok {
		return
	}

	callBack, found := subscriptions.subscriptionByRequestID[requestID]
	if !found {
		zlog.Error("cannot find websocket message handler for a new stream.... this should not happen",
			zap.Uint64("request_id", requestID),
			zap.Uint64("subscription_id", subID),
		)
		return
	}
	callBack.subID = subID
	subscriptions.subscriptionByWSSubID[subID] = callBack

	zlog.Debug("registered ws subscription",
		zap.Uint64("subscription_id", subID),
		zap.Uint64("request_id", requestID),
		zap.Int("subscription_count", len(subscriptions.subscriptionByWSSubID)),
	)
	return
}

func (c *Client) handleSubscriptionMessage(subID uint64, message []byte) {
	if traceEnabled {
		zlog.Debug("received subscription message",
			zap.Uint64("subscription_id", subID),
		)
	}

	c.lock.RLock()
	subscriptions, ok := c.connToSubscriptions[c.conn]
	if !ok {
		return
	}
	sub, found := subscriptions.subscriptionByWSSubID[subID]
	c.lock.RUnlock()
	if !found {
		// Origin of the message might be of cleaned up connection.
		zlog.Info("unable to find subscription for ws message", zap.Uint64("subscription_id", subID))
		return
	}

	// Decode the message using the subscription-provided decoderFunc.
	result, err := sub.decoderFunc(message)
	if err != nil {
		fmt.Println("*****************************")
		c.closeSubscription(sub.req.ID, fmt.Errorf("unable to decode client response: %w", err))
		return
	}

	// this cannot be blocking or else
	// we  will no read any other message
	if len(sub.stream) >= cap(sub.stream) {
		zlog.Warn("closing ws client subscription... not consuming fast en ought",
			zap.Uint64("request_id", sub.req.ID),
		)
		c.closeSubscription(sub.req.ID, fmt.Errorf("reached channel max capacity %d", len(sub.stream)))
		return
	}

	if !sub.closed {
		sub.stream <- result
	}
	return
}

func (c *Client) closeAllSubscription(conn *websocket.Conn, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	subs, ok := c.connToSubscriptions[conn]
	if !ok {
		return
	}
	for _, sub := range subs.subscriptionByRequestID {
		sub.err <- err
	}
	subs.subscriptionByRequestID = nil
	subs.subscriptionByWSSubID = nil
	delete(c.connToSubscriptions, conn)
}

func (c *Client) closeSubscription(reqID uint64, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	subs, ok := c.connToSubscriptions[c.conn]
	if !ok {
		return
	}

	sub, found := subs.subscriptionByRequestID[reqID]
	if !found {
		return
	}

	sub.err <- err

	err = c.unsubscribe(sub.subID, sub.unsubscribeMethod)
	if err != nil {
		zlog.Warn("unable to send rpc unsubscribe call",
			zap.Error(err),
		)
	}

	delete(subs.subscriptionByRequestID, sub.req.ID)
	delete(subs.subscriptionByWSSubID, sub.subID)
}

func (c *Client) unsubscribe(subID uint64, method string) error {
	req := newRequest([]interface{}{subID}, method, nil)
	data, err := req.encode()
	if err != nil {
		return fmt.Errorf("unable to encode unsubscription message for subID %d and method %s", subID, method)
	}
	ctx, timeout := context.WithTimeout(context.Background(), writeWait)
	defer timeout()
	err = c.write(ctx, data, false)
	if err != nil {
		return fmt.Errorf("unable to send unsubscription message for subID %d and method %s", subID, method)
	}
	return nil
}

func (c *Client) subscribe(
	params []interface{},
	conf map[string]interface{},
	subscriptionMethod string,
	unsubscribeMethod string,
	decoderFunc decoderFunc,
) (*Subscription, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	req := newRequest(params, subscriptionMethod, conf)
	data, err := req.encode()
	if err != nil {
		return nil, fmt.Errorf("subscribe: unable to encode subsciption request: %w", err)
	}

	sub := newSubscription(
		req,
		func(err error) {
			c.closeSubscription(req.ID, err)
		},
		unsubscribeMethod,
		decoderFunc,
	)

	zlog.Info("added new subscription to websocket client", zap.Int("count", len(c.connToSubscriptions)))
	ctx, cancel := context.WithTimeout(context.Background(), writeWait)
	defer cancel()
	err = c.write(ctx, data, false)
	if err != nil {
		return nil, fmt.Errorf("unable to write request: %w", err)
	}
	// Safe to move after the write, as the lock will block reads.
	// This ensures the subscription ID is retained during reconnection.
	subs := c.connToSubscriptions[c.conn]
	subs.subscriptionByRequestID[req.ID] = sub

	return sub, nil
}

func decodeResponseFromReader(r io.Reader, reply interface{}) (err error) {
	var c *response
	if err := json.NewDecoder(r).Decode(&c); err != nil {
		return err
	}

	if c.Error != nil {
		jsonErr := &json2.Error{}
		if err := json.Unmarshal(*c.Error, jsonErr); err != nil {
			return &json2.Error{
				Code:    json2.E_SERVER,
				Message: string(*c.Error),
			}
		}
		return jsonErr
	}

	if c.Params == nil {
		return json2.ErrNullResult
	}

	return json.Unmarshal(*c.Params.Result, &reply)
}

func decodeResponseFromMessage(r []byte, reply interface{}) (err error) {
	var c *response
	if err := json.Unmarshal(r, &c); err != nil {
		return err
	}

	if c.Error != nil {
		jsonErr := &json2.Error{}
		if err := json.Unmarshal(*c.Error, jsonErr); err != nil {
			return &json2.Error{
				Code:    json2.E_SERVER,
				Message: string(*c.Error),
			}
		}
		return jsonErr
	}

	if c.Params == nil {
		return json2.ErrNullResult
	}

	return json.Unmarshal(*c.Params.Result, &reply)
}
