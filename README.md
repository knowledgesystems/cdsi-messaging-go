# cdsi-messaging-go

A wrapper around [NATS - Go Client](https://github.com/nats-io/nats.go).

## Basic Usage

```go

import (
	cdsim "github.com/knowledgesystems/cdsi-messaging-go"
)

// without TLS
m, err := cdsim.NewNatsMessaging("localhost:4222")

// with TLS
m, err := cdsim.NewNatsMessaging("localhost:4222", cdsim.WithTLS(certPath, keyPath, userId, pw))
if err != nil {
	// do something
}

// publish a message
err = m.Publish("subject", []byte("Hello World"))
if err != nil {
	// do something	
}

// subscribe to a subject
// consumer id much match an authorized id setup in NATS/Jetstream configuration 
m.Subscribe("consumer id", " subject", func(m *cdsim.Msg) {
	fmt.Println("Subscriber received an message via NATS:", string(m.Data))
})

m.Shutdown()
```
