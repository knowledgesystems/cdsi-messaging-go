package cdsi_messaging

import (
	"github.com/nats-io/nats.go"
	"time"
)

const (
	pullMsgSize = 500
)

// These types are here so clients do not have to depend on nats libs when subscribing
type Msg struct {
	Subject string
	Data    []byte
}
type MsgHandler func(msg *Msg)

// The main type used by clients
type NatsMessaging struct {
	nc *nats.Conn
	js nats.JetStream
}

// Options for Messaging provider
type Options struct {
	UseTLS      bool
	TLSCertPath string
	TLSKeyPath  string
	UserId      string
	Password    string
}

// An Option is a function operating on the Messaging Options
type Option func(*Options)

// WithTLS is an Option to enable a TLS channel
func WithTLS(certPath, keyPath, userId, pw string) Option {
	return func(o *Options) {
		o.UseTLS = true
		o.TLSCertPath = certPath
		o.TLSKeyPath = keyPath
		o.UserId = userId
		o.Password = pw
	}
}

func NewNatsMessaging(url string, opts ...Option) (*NatsMessaging, error) {
	var options Options
	for _, o := range opts {
		o(&options)
	}

	var nc *nats.Conn
	var err error
	if options.UseTLS {
		cert := nats.ClientCert(options.TLSCertPath, options.TLSKeyPath)
		nc, err = nats.Connect(url, cert, nats.UserInfo(options.UserId, options.Password))
		if err != nil {
			return nil, err
		}
	} else {
		nc, err = nats.Connect(url)
		if err != nil {
			return nil, err
		}
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	return &NatsMessaging{
		nc: nc,
		js: js,
	}, nil
}

func (m *NatsMessaging) Publish(subj string, data []byte) error {
	msg := nats.NewMsg(subj)
	msg.Data = data
	msg.Header.Add("Nats-Msg-Subject", subj)

	_, err := m.js.PublishMsg(msg)
	if err != nil {
		return err
	}
	select {
	case <-m.js.PublishAsyncComplete():
	}
	return err
}

func (m *NatsMessaging) Subscribe(con string, subj string, mh MsgHandler) error {

	// lets create a nats messsage handler
	// that passes the nats message content
	// to the smile message handler
	nmh := func(m *nats.Msg) {
		sm := &Msg{
			Subject: m.Subject,
			Data:    m.Data,
		}
		mh(sm)
	}

	// subscribe to the Nats subject & register the nats message handler
	_, err := m.js.Subscribe(subj, nmh, nats.Durable(con))
	return err
}

func (m *NatsMessaging) PullFromDate(t time.Time, strm string, subj string) ([]*nats.Msg, error) {
	s, err := m.js.PullSubscribe(subj, "", nats.BindStream(strm), nats.StartTime(t))
	if err != nil {
		return nil, err
	}
	msgs, err := s.Fetch(pullMsgSize, nats.MaxWait(5*time.Second))
	if err != nil {
		return nil, err
	}
	for _, msg := range msgs {
		msg.Ack()
	}
	return msgs, nil
}

func (m *NatsMessaging) Shutdown() {
	m.nc.Flush()
	m.nc.Close()
	m.nc = nil
	m.js = nil
}
