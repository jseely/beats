package amqp10

import "github.com/elastic/beats/libbeat/outputs/codec"

type Config struct {
	Address       string       `config:"address" validate:"required"`
	AccessKeyName string       `config:"access_key_name" validate:"required"`
	AccessKey     string       `config:"access_key" validate:"required"`
	QueueName     string       `config:"queue" validate:"required"`
	Codec         codec.Config `config:"codec"`
	Pretty        bool         `config:"pretty"`
}

var defaultConfig = Config{}
