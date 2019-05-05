package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Errors returned by broker
var (
	ErrBrokerClosed = errors.New("broker closed")
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
		onResponse:      c.OnResponse,
		onResponseError: c.OnResponseError,
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
	Request       Request
}

// Broker represents a single Kafka broker
type Broker struct {
	clientID      string
	conn          net.Conn
	Versions      SupportedVersions
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
func (b *Broker) SendRequest(ctx context.Context, ws Request) error {
	b.sendMutex.Lock()
	defer b.sendMutex.Unlock()

	select {
	case <-b.close:
		return ErrBrokerClosed
	default:
	}

	cid := b.correlationID
	b.correlationID++

	err := b.sendRequestSync(ctx, cid, ws)
	if err != nil {
		return err
	}

	b.sentRequests <- sentRequest{
		CorrelationID: cid,
		Request:       ws,
	}

	return nil
}

func (b *Broker) sendRequestSync(ctx context.Context, correlationID int32, ws Request) error {
	var h *requestHeader

	if ws.Version() >= 0 {
		h = &requestHeader{
			APIKey:        ws.APIKey(),
			APIVersion:    ws.Version(),
			CorrelationID: correlationID,
			ClientID:      b.clientID,
		}
	}

	r := &request{
		h: h,
		r: ws,
	}

	if deadline, ok := ctx.Deadline(); ok {
		b.conn.SetWriteDeadline(deadline)
	}

	return r.write(b.conn)
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
			b.onResponseError(newUncorrelatedResponseError(cid, correlatedRequest.CorrelationID))
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
	b.conn.SetReadDeadline(time.Now().Add(b.readTimeout))

	size, err := readInt32(b.conn)
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

	cid, err := readInt32(buf)
	return cid, buf, err
}

func (b *Broker) shipResponse(request sentRequest, resp *byteBuffer) error {
	defer resp.Close()

	var (
		v   interface{}
		err error
	)

	switch request.Request.APIKey() {
	case APIKeyMetadata:
		v, err = readMetadataResponse(request.Request.Version(), resp)
	default:
		return newClientUnsupportedAPIError(request.Request.APIKey(), request.Request.Version())
	}

	if err != nil {
		return err
	}

	go b.onResponse(request.Request.APIKey(), request.Request.Version(), v)

	return nil
}

func (b *Broker) readAPISupport() error {
	err := b.sendRequestSync(context.Background(), apiCID, NewAPIVersionsRequestV1())
	if err != nil {
		return err
	}

	cid, resp, err := b.readResponse(true)
	if err != nil {
		return err
	}

	if cid != apiCID {
		return newUncorrelatedResponseError(apiCID, cid)
	}

	versionResp, err := readAPIVersionsResponseV1(resp)
	if err != nil {
		return err
	}

	b.Versions = versionResp.versions
	return nil
}

func (b *Broker) saslAuth(c SASLConfig) error {
	const (
		plainMechanism = "PLAIN"
	)
	if c.Mechaism != plainMechanism {
		return fmt.Errorf("unsupported SASL Mechanism %q. Supported: %q", c.Mechaism, plainMechanism)
	}

	if !b.Versions.IsSupported(APIKeySaslHandshake, 0) {
		return newServerUnsupportedAPIError(APIKeySaslHandshake, b.Versions[APIKeySaslHandshake])
	}

	handshakeRequest := newSASLHandshakeRequestV0(plainMechanism)

	err := b.sendRequestSync(context.Background(), authCID, handshakeRequest)

	cid, resp, err := b.readResponse(true)
	if err != nil {
		return err
	}

	if cid != authCID {
		return newUncorrelatedResponseError(authCID, cid)
	}

	_, err = readSASLHandshakeResponseV0(resp)
	if err != nil {
		return err
	}

	authRequest := NewSASLAuthenticateRequest(c.Plain.UserName, c.Plain.Password)

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

// RequestMetadata sends a request for metadata. If the API supports it, allowAutoTopicCreation will be sent as false
func (b *Broker) RequestMetadata(ctx context.Context, topics []string) error {
	if b.Versions.IsSupported(APIKeyMetadata, 5) {
		return b.SendRequest(ctx, newMetadataRequestV5(topics, false))
	}

	return newServerUnsupportedAPIError(APIKeyMetadata, b.Versions[APIKeyMetadata])
}

// RequestMetadataWithAllowTopicCreation sends a request including topic creation or error if the require API isn't supported
func (b *Broker) RequestMetadataWithAllowTopicCreation(ctx context.Context, topics []string, allowAutoTopicCreation bool) error {
	if b.Versions.IsSupported(APIKeyMetadata, 5) {
		return b.SendRequest(ctx, newMetadataRequestV5(topics, allowAutoTopicCreation))
	}

	return newServerUnsupportedAPIError(APIKeyMetadata, b.Versions[APIKeyMetadata])
}
