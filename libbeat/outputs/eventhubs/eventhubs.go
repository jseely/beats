package eventhubs

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

	"github.com/Azure/azure-amqp-common-go/sas"

	"github.com/Azure/azure-event-hubs-go"
)

type amqpClient struct {
	codec    codec.Codec
	index    string
	observer outputs.Observer
	client   *eventhub.Hub
	config   *common.Config
}

func init() {
	outputs.RegisterType("eventhubs", makeEventHubClient)
}

func makeEventHubClient(beat beat.Info, observer outputs.Observer, cfg *common.Config) (outputs.Group, error) {
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

	tokenProvider, err := sas.NewTokenProvider(sas.TokenProviderWithNamespaceAndKey(config.Namespace, config.KeyName, config.Key))
	if err != nil {
		return outputs.Fail(err)
	}

	client, err := eventhub.NewHub(config.Namespace, config.Hub, tokenProvider)
	if err != nil {
		return outputs.Fail(err)
	}

	a := &amqpClient{
		index:    index,
		observer: observer,
		codec:    enc,
		client:   client,
		config:   cfg,
	}

	return outputs.Success(1, 5, a)
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

	for i := range events {
		err := a.publishEvent(&events[i])
		if err != nil {
			events = events[i:]

			// Return events to pipeline to be retried
			batch.RetryEvents(events)
			logp.Err("Failed to publish events caused by: %s", err.Error())

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
		logp.Critical("Unable to encode event: %s", err.Error())
		return err
	}

	messageId := uuid.NewV4()

	msg := &eventhub.Event{
		Data: serializedEvent,
		ID:   messageId.String(),
	}
	if len(a.config.Properties) > 0 {
		msg.Properties = make(map[string]interface{})
		for k, v := range a.config.Properties {
			msg.Properties[k] = v
		}
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	err = a.client.Send(ctx, msg)
	if err != nil {
		logp.Critical("Unable to send event: %s", err.Error())
		return err
	}
	return nil
}
