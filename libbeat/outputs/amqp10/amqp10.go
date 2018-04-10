package amqp10

import (
	"context"
	"time"

	"github.com/satori/go.uuid"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/outputs/codec/json"
	"github.com/elastic/beats/libbeat/publisher"

	"pack.ag/amqp"
)

type amqpClient struct {
	codec         codec.Codec
	index         string
	observer      outputs.Observer
	address       string
	queue         string
	accessKeyName string
	accessKey     string
	client        *amqp.Client
	session       *amqp.Session
	sender        *amqp.Sender
}

func init() {
	outputs.RegisterType("amqp10", makeAmqpClient)
}

func makeAmqpClient(beat beat.Info, observer outputs.Observer, cfg *common.Config) (outputs.Group, error) {
	config := defaultConfig
	err := cfg.Unpack(&config)
	if err != nil {
		return outputs.Fail(err)
	}

	var enc codec.Codec
	if config.Codec.Namespace.IsSet() {
		enc, err = codec.CreateEncoder(beat, config.Codec)
		if err != nil {
			return outputs.Fail(err)
		}
	} else {
		enc = json.New(config.Pretty, beat.Version)
	}

	index := beat.Beat
	a := &amqpClient{
		index:         index,
		observer:      observer,
		codec:         enc,
		address:       config.Address,
		queue:         config.QueueName,
		accessKeyName: config.AccessKeyName,
		accessKey:     config.AccessKey,
	}

	return outputs.Success(100, 5, a)
}

func (a *amqpClient) Close() error {
	if a.client != nil {
		return a.client.Close()
	} else {
		return nil
	}
}

func (a *amqpClient) Publish(batch publisher.Batch) error {
	st := a.observer
	events := batch.Events()

	st.NewBatch(len(events))

	if len(events) == 0 {
		batch.ACK()
		return nil
	}

	// If sender is nil try to refresh
	var err error
	if a.sender == nil {
		if err = a.refreshLink(); err != nil {
			batch.RetryEvents(events)
			return err
		}
	}

	for i := range events {
		err := a.publishEvent(&events[i])
		if err != nil {
			events = events[i:]

			// Return events to pipeline to be retried
			batch.RetryEvents(events)
			logp.Err("Failed to publish events caused by: %v", err)

			// Record Stats
			st.Acked(i)
			st.Failed(len(events))
			return err
		}
	}

	// Ack that the batch has been sent
	batch.ACK()

	// Record stats
	st.Acked(len(events))
	return nil
}

func (a *amqpClient) publishEvent(event *publisher.Event) error {
	serializedEvent, err := a.codec.Encode(a.index, &event.Content)
	if err != nil {
		if !event.Guaranteed() {
			return err
		}
		logp.Critical("Unable to encode event: %v", err)
		return err
	}

	messageId, err := uuid.NewV4()
	if err != nil {
		if !event.Guaranteed() {
			return err
		}
		logp.Critical("Unable to create new UUID: %v", err)
		return err
	}
	message := &amqp.Message{
		Data: [][]byte{serializedEvent},
		ApplicationProperties: map[string]interface{}{
			"Type":      "Beats",
			"Beat":      a.index,
			"MessageId": messageId.String(),
			"Timestamp": event.Content.Timestamp.Format("2006-01-02T15:04:05.000+00:00"),
		},
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	err = a.sender.Send(ctx, message)
	if err != nil {
		logp.Critical("Unable to send event: %v", err)
		// Error may be a connection issue, close the connection so it can be recreated
		a.client.Close()
		a.client = nil
		a.sender = nil
		a.session = nil
		return err
	}
	return nil
}

func (a *amqpClient) refreshLink() error {
	var err error
	if a.client == nil {
		a.session = nil
		a.sender = nil
		a.client, err = amqp.Dial(a.address, amqp.ConnSASLPlain(a.accessKeyName, a.accessKey))
		if err != nil {
			a.client = nil
			return err
		}
	}

	if a.session == nil {
		a.sender = nil
		a.session, err = a.client.NewSession()
		if err != nil {
			// If we fail to make a session just restart from dialing the next time refreshLink is called
			a.client.Close()
			a.client = nil
			return err
		}
	}

	if a.sender == nil {
		a.sender, err = a.session.NewSender(amqp.LinkTargetAddress("/" + a.queue))
		if err != nil {
			// If we fail to make a sender just restart from dialing the next time refreshLink is called
			a.client.Close()
			a.client = nil
			return err
		}
	}
	return nil
}
