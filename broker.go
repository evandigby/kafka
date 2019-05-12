package kafka

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/evandigby/kafka/api"
	"github.com/evandigby/kafka/api/metadata"
	"github.com/evandigby/kafka/api/sasl"
	"github.com/evandigby/kafka/api/versions"
	"github.com/evandigby/kafka/api/enc"
)

// Errors returned by broker
var (
	ErrBrokerClosed         = errors.New("broker closed")
	ErrUncorrelatedResponse = errors.New("uncorrelated response")
)

const (
	apiCID  = 10
	authCID = 11
)

// NewBroker connects to a new Kafka broker.
func NewBroker(addr string, c BrokerConfig) (*Broker, error) {
	c = configDefaults(c)

	conn, err := tls.Dial("tcp", addr, nil)
	if err != nil {
		return nil, err
	}

	broker := &Broker{
		conn:            conn,
		clientID:        c.ClientID,
		correlationID:   100,
		readTimeout:     c.ReadTimeout,
		sendTimeout:     c.SendTimeout,
		sentRequests:    make(chan sentRequest, c.RequestQueueSize),
		close:           make(chan struct{}),
		readClosed:      make(chan struct{}),
		onResponse:      c.Response.OnResponse,
		onResponseError: c.Response.OnResponseError,
	}

	err = broker.readAPISupport()
	if err != nil {
		return nil, err
	}

	if c.SASL.Enabled {
		err := broker.saslAuth(c.SASL)
		if err != nil {
			return nil, err
		}
	}

	go broker.readResponses()

	return broker, nil
}

type sentRequest struct {
	CorrelationID int32
	Request       api.Request
}

// Broker represents a single Kafka broker
type Broker struct {
	clientID      string
	conn          net.Conn
	Versions      versions.Supported
	correlationID int32
	readTimeout   time.Duration
	sendTimeout   time.Duration

	sentRequests chan sentRequest

	onResponse      OnResponse
	onResponseError OnResponseError

	sendMutex  sync.Mutex
	close      chan struct{}
	readClosed chan struct{}
}

// Close closes the connection to a Kafka broker
func (b *Broker) Close() error {
	b.sendMutex.Lock()
	close(b.close)
	b.sendMutex.Unlock()

	defer b.conn.Close()

	<-b.readClosed
	return b.conn.Close()
}

// SendRequest sends a request to the broker
func (b *Broker) SendRequest(ctx context.Context, req api.Request) error {
	b.sendMutex.Lock()
	defer b.sendMutex.Unlock()

	select {
	case <-b.close:
		return ErrBrokerClosed
	default:
	}

	cid := b.correlationID
	b.correlationID++

	err := b.sendRequestSync(ctx, cid, req)
	if err != nil {
		return err
	}

	b.sentRequests <- sentRequest{
		CorrelationID: cid,
		Request:       req,
	}

	return nil
}

func (b *Broker) sendRequestSync(ctx context.Context, correlationID int32, req api.Request) error {
	var h *api.RequestHeader

	if req.Version() >= 0 {
		h = &api.RequestHeader{
			APIKey:        req.APIKey(),
			APIVersion:    req.Version(),
			CorrelationID: correlationID,
			ClientID:      b.clientID,
		}
	}

	r := &api.RequestData{
		Header:  h,
		Request: req,
	}

	if deadline, ok := ctx.Deadline(); ok {
		err := b.conn.SetWriteDeadline(deadline)
		if err != nil {
			return err
		}
	}

	buf := bytes.NewBuffer(nil)

	r.Write(buf)

	return r.Write(b.conn)
}

func (b *Broker) readResponses() {
	defer close(b.readClosed)
	for {
		select {
		case <-b.close:
			return
		default:
		}

		cid, resp, err := b.readResponse(true)
		if err != nil {
			if nerr, ok := err.(net.Error); ok {
				if nerr.Temporary() || nerr.Timeout() {
					continue
				}
			}

			b.onResponseError(err)
			if err == io.EOF {
				return
			}

			continue
		}

		correlatedRequest, ok := <-b.sentRequests
		if !ok {
			// closed
			return
		}

		if cid != correlatedRequest.CorrelationID {
			b.onResponseError(fmt.Errorf("reading response: %w", ErrUncorrelatedResponse))
			continue
		}

		err = b.shipResponse(correlatedRequest, resp)
		if err != nil {
			b.onResponseError(err)
			continue
		}
	}
}

func (b *Broker) readResponse(readCorrelation bool) (int32, *byteBuffer, error) {
	err := b.conn.SetReadDeadline(time.Now().Add(b.readTimeout))
	if err != nil {
		return -1, nil, err
	}

	size, err := enc.ReadInt32(b.conn)
	if err != nil {
		return -1, nil, err
	}

	if size == 0 {
		return -1, nil, nil
	}

	buf := newBuffer()

	_, err = io.CopyN(buf, b.conn, int64(size))
	if err != nil {
		return -1, nil, err
	}

	if !readCorrelation {
		return -1, buf, nil
	}

	cid, err := enc.ReadInt32(buf)
	return cid, buf, err
}

func (b *Broker) shipResponse(request sentRequest, resp *byteBuffer) error {
	defer resp.Close()

	var (
		responseValue interface{}
		err           error
	)

	switch request.Request.APIKey() {
	case api.KeyMetadata:
		responseValue, err = metadata.ReadResponse(resp, request.Request.Version())
	default:
		return fmt.Errorf("%v: %w", request.Request.APIKey(), versions.ErrorUnsupportedAPI)
	}

	if err != nil {
		return err
	}

	go b.onResponse(request.Request.APIKey(), request.Request.Version(), responseValue)

	return nil
}

func (b *Broker) readAPISupport() error {
	err := b.sendRequestSync(context.Background(), apiCID, &versions.Request{})
	if err != nil {
		return err
	}

	cid, resp, err := b.readResponse(true)
	if err != nil {
		return err
	}

	if cid != apiCID {
		return fmt.Errorf("read api support: %w", ErrUncorrelatedResponse)
	}

	versionResp, err := versions.ReadV1Response(resp)
	if err != nil {
		return err
	}

	b.Versions = versionResp.Versions
	return nil
}

func (b *Broker) saslAuth(c SASLConfig) error {
	const (
		plainMechanism = "PLAIN"
	)
	if c.Mechaism != plainMechanism {
		return fmt.Errorf("unsupported SASL Mechanism %q. Supported: %q", c.Mechaism, plainMechanism)
	}

	if !b.Versions.IsSupported(api.KeySaslHandshake, 0) {
		return fmt.Errorf("ssl auth: %v: %w", api.KeySaslHandshake, versions.ErrorUnsupportedAPI)
	}

	handshakeRequest := sasl.NewHandshakeRequestV0(plainMechanism)
	err := b.sendRequestSync(context.Background(), authCID, handshakeRequest)
	cid, resp, err := b.readResponse(true)
	if err != nil {
		return err
	}

	if cid != authCID {
		return fmt.Errorf("sasl auth handshake: %w", ErrUncorrelatedResponse)
	}

	_, err = sasl.ReadHandshakeResponseV0(resp)
	if err != nil {
		return err
	}

	authRequest := sasl.NewAuthenticateRequest(c.Plain.UserName, c.Plain.Password)

	err = b.sendRequestSync(context.Background(), -1, authRequest)
	if err != nil {
		return err
	}

	_, resp, err = b.readResponse(false)
	if err != nil {
		return err
	}

	return nil
}

// RequestMetadata sends a request including topic creation or error if the required API isn't supported
func (b *Broker) RequestMetadata(ctx context.Context, topics []string, allowAutoTopicCreation bool) error {
	r, err := b.Versions.Request(api.KeyMetadata, &metadata.Request{
		Topics:             topics,
		AllowTopicCreation: allowAutoTopicCreation,
	})
	if err != nil {
		return err
	}

	return b.SendRequest(ctx, r)
}
