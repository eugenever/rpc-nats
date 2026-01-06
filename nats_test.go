package rpcnats

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func NewId() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func defaultNatsOpt(o *nats.Options) error {
	o.AllowReconnect = true
	o.MaxReconnect = -1
	o.ReconnectWait = 10 * time.Millisecond
	o.ReconnectJitter = 1 * time.Millisecond
	o.Timeout = 10 * time.Millisecond
	return nil
}

func newNatsServer(port int, storageDir ...string) (*server.Server, error) {
	var dir string
	var err error
	if len(storageDir) > 0 {
		dir = storageDir[0]
	} else {
		dir, err = NewId()
		if err != nil {
			return nil, err
		}
	}
	opts := server.Options{
		Host:      "localhost",
		Port:      port,
		Debug:     true,
		Trace:     true,
		JetStream: true,
		StoreDir:  fmt.Sprintf("/tmp/nats/%s", dir),
	}
	ns, err := server.NewServer(&opts)
	if err != nil {
		return nil, err
	}
	return ns, nil
}

func TestNatsClientConnect(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ns, err := newNatsServer(1222)
		require.Nil(t, err)
		defer ns.Shutdown()
		ns.Start()
		time.Sleep(50 * time.Millisecond)

		client := NewNatsClient(WithNats("localhost", 1222, defaultNatsOpt))
		err = client.Connect()
		require.Nil(t, err)
	})

	t.Run("error", func(t *testing.T) {
		client := NewNatsClient(WithNats("localhost", 1222, defaultNatsOpt))
		err := client.Connect()
		require.NotNil(t, err)
	})

	t.Run("check_connecting_loop", func(t *testing.T) {
		ns1, err := newNatsServer(1222)
		require.Nil(t, err)
		defer ns1.Shutdown()

		ns2, err := newNatsServer(1333)
		require.Nil(t, err)
		defer ns2.Shutdown()

		ns1.Start()
		time.Sleep(50 * time.Millisecond)

		client := NewNatsClient(
			WithNats("localhost", 1222, defaultNatsOpt),
			WithNats("localhost", 1333, defaultNatsOpt),
			WithConnectTimeout(10*time.Millisecond),
		)
		require.Equal(t, 2, len(client.natsConfigs))
		err = client.Connect()
		require.Nil(t, err)
		require.Equal(t, 1, len(client.natsConfigs))

		ns2.Start()
		time.Sleep(50 * time.Millisecond)

		require.Equal(t, 0, len(client.natsConfigs))
	})

	t.Run("isLive", func(t *testing.T) {
		ns1, err := newNatsServer(1222)
		require.Nil(t, err)
		defer ns1.Shutdown()

		ns1.Start()
		time.Sleep(50 * time.Millisecond)

		client := NewNatsClient(
			WithNats("localhost", 1222, defaultNatsOpt),
			WithConnectTimeout(10*time.Millisecond),
		)
		err = client.Connect()
		require.Nil(t, err)
		require.Equal(t, 0, len(client.natsConfigs))
		defer client.Stop()

		for _, v := range client.connections {
			require.True(t, v.isLive)
		}

		ns1.Shutdown()
		time.Sleep(30 * time.Millisecond)

		for _, v := range client.connections {
			require.False(t, v.isLive)
		}

		ns2, err := newNatsServer(1222)
		require.Nil(t, err)
		defer ns2.Shutdown()

		ns2.Start()
		time.Sleep(50 * time.Millisecond)

		for _, v := range client.connections {
			require.True(t, v.isLive)
		}
	})

}

func TestNatsClientCall(t *testing.T) {

	t.Run("no_responders", func(t *testing.T) {
		ns1, err := newNatsServer(1222)
		require.Nil(t, err)
		defer ns1.Shutdown()

		ns2, err := newNatsServer(1333)
		require.Nil(t, err)
		defer ns2.Shutdown()

		ns1.Start()
		ns2.Start()

		time.Sleep(50 * time.Millisecond)

		client := NewNatsClient(
			WithNats("localhost", 1222, defaultNatsOpt),
			WithNats("localhost", 1333, defaultNatsOpt),
		)
		err = client.Connect()
		require.Nil(t, err)

		res, err := client.Call("111", []byte{10}, 100*time.Millisecond)
		require.ErrorIs(t, err, nats.ErrNoResponders)
		require.Nil(t, res)
	})

	t.Run("success_respose", func(t *testing.T) {
		ns1, err := newNatsServer(1222)
		require.Nil(t, err)
		defer ns1.Shutdown()

		ns2, err := newNatsServer(1333)
		require.Nil(t, err)
		defer ns2.Shutdown()

		ns1.Start()
		ns2.Start()

		time.Sleep(50 * time.Millisecond)

		client := NewNatsClient(
			WithNats("localhost", 1222, defaultNatsOpt),
			WithNats("localhost", 1333, defaultNatsOpt),
			WithSubscription("subj1", "q1", func(m *nats.Msg) {
				err := m.Respond([]byte{10})
				require.Nil(t, err)
			}),
		)
		err = client.Connect()
		require.Nil(t, err)
		defer client.Stop()

		res, err := client.Call("subj1", []byte{1}, 50*time.Millisecond)
		require.Nil(t, err)
		require.Equal(t, []byte{10}, res)
	})

}
