package rpcnats

import (
	"time"

	"github.com/nats-io/nats.go"
)

type optFunc func(*NatsClient)

func WithNats(host string, port int, natsOptions nats.Option) optFunc {
	return func(client *NatsClient) {
		client.natsConfigs = append(client.natsConfigs, natsConfig{
			host:    host,
			port:    port,
			options: natsOptions,
		})
	}
}

func WithSubscription(subj string, queue string, fn func(*nats.Msg)) optFunc {
	return func(client *NatsClient) {
		client.rpcConfigs = append(client.rpcConfigs, rpcConfig{subj: subj, queue: queue, fn: fn})
	}
}

func WithLogError(fn func(string)) optFunc {
	return func(client *NatsClient) {
		if fn != nil {
			client.logError = fn
		}
	}
}

func WithLogWarn(fn func(string)) optFunc {
	return func(client *NatsClient) {
		if fn != nil {
			client.logWarn = fn
		}
	}
}

func WithLogInfo(fn func(string)) optFunc {
	return func(client *NatsClient) {
		if fn != nil {
			client.logInfo = fn
		}
	}
}

func WithConnectTimeout(timeout time.Duration) optFunc {
	return func(client *NatsClient) {
		client.connectTimeout = timeout
	}
}
