package eventhubs

import "github.com/elastic/beats/libbeat/outputs/codec"

type Config struct {
	Namespace  string            `config:"namespace" validate:"required"`
	Hub        string            `config:"hub" validate:"required"`
	KeyName    string            `config:"key_name" validate:"required"`
	Key        string            `config:"key" validate:"required"`
	Codec      codec.Config      `config:"codec"`
	Pretty     bool              `config:"pretty"`
	Properties map[string]string `config:"properties"`
}

var defaultConfig = Config{}
