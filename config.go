package main

type Config struct {
	CollectionInterval string `env:"COLLECTION_INTERVAL" default:"1s"`
	BufferSize         int    `env:"BUFFER_SIZE" default:"1000"`
}
