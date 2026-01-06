package rpcnats

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	ErrAllConnectionsLost = errors.New("all connections lost")
)

type NatsClient struct {
	natsConfigs []natsConfig
	rpcConfigs  []rpcConfig

	connectionsMutex *sync.RWMutex
	connections      map[*nats.Conn]*natsConnectionDescriptor

	stop           chan struct{}
	logError       func(string)
	logWarn        func(string)
	logInfo        func(string)
	connectTimeout time.Duration
}

type natsConfig struct {
	host    string
	port    int
	options nats.Option
}

type rpcConfig struct {
	subj  string
	queue string
	fn    func(msg *nats.Msg)
}

type natsConnectionDescriptor struct {
	conn             *nats.Conn
	rpcSubscriptions []*nats.Subscription
	isLive           bool
}

func NewNatsClient(options ...optFunc) *NatsClient {
	client := &NatsClient{
		natsConfigs: []natsConfig{},
		rpcConfigs:  []rpcConfig{},

		connectionsMutex: &sync.RWMutex{},
		connections:      map[*nats.Conn]*natsConnectionDescriptor{},
		stop:             make(chan struct{}),
		logError:         func(s string) {},
		logWarn:          func(s string) {},
		logInfo:          func(s string) {},
		connectTimeout:   2000 * time.Millisecond,
	}
	for _, optFunc := range options {
		optFunc(client)
	}
	return client
}

// Connect try to connect to previously configured servers. If some connections
// does not established Connect start reconecting loop in background and return nil.
// If no one connections has been established Connect returns last error.
func (n *NatsClient) Connect() error {
	credentialsToBeRemoved := []natsConfig{}
	var err2 error
	for _, cred := range n.natsConfigs {
		connDesc, err := n.natsConnect(cred)
		if err != nil {
			err2 = err
			continue
		}

		n.connectionsMutex.Lock()
		n.connections[connDesc.conn] = connDesc
		n.connectionsMutex.Unlock()

		credentialsToBeRemoved = append(credentialsToBeRemoved, cred)
	}
	n.natsConfigs = slices.DeleteFunc(n.natsConfigs, func(cred natsConfig) bool {
		for _, oldCred := range credentialsToBeRemoved {
			if oldCred.host == cred.host && oldCred.port == cred.port {
				return true
			}
		}
		return false
	})
	if len(n.natsConfigs) > 0 {
		go n.connectingLoop()
	}
	if len(credentialsToBeRemoved) == 0 {
		return err2
	}
	return nil
}

// Stop graceful shutdowns all connections
func (n *NatsClient) Stop() {
	close(n.stop)
	n.connectionsMutex.RLock()
	connDescriptors := slices.Collect(maps.Values(n.connections))
	n.connectionsMutex.RUnlock()

	for _, connDesc := range connDescriptors {
		for _, rpcSubs := range connDesc.rpcSubscriptions {
			rpcSubs.Drain()
			rpcSubs.Unsubscribe()
		}
		connDesc.conn.Drain()
		connDesc.conn.Close()
	}
}

func (n *NatsClient) Call(subj string, data []byte, timeout time.Duration) ([]byte, error) {
	connDescriptors := n.liveConnections()
	numberOfConnections := len(connDescriptors)
	if numberOfConnections == 0 {
		return nil, ErrAllConnectionsLost
	}
	var (
		err error
		msg *nats.Msg
	)
	// TODO: add random connections enumeration
	for i := range numberOfConnections {
		conn := connDescriptors[i].conn
		msg, err = conn.Request(subj, data, timeout)
		if err != nil {
			n.logWarn(err.Error())
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func (n *NatsClient) connectingLoop() {
	ticker := time.NewTicker(n.connectTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			credentialsToBeRemoved := []natsConfig{}
			for _, cred := range n.natsConfigs {
				connDesc, err := n.natsConnect(cred)
				if err != nil {
					n.logError(err.Error())
					continue
				}

				n.connectionsMutex.Lock()
				n.connections[connDesc.conn] = connDesc
				n.connectionsMutex.Unlock()

				credentialsToBeRemoved = append(credentialsToBeRemoved, cred)
			}
			n.natsConfigs = slices.DeleteFunc(n.natsConfigs, func(cred natsConfig) bool {
				for _, oldCred := range credentialsToBeRemoved {
					if oldCred.host == cred.host && oldCred.port == cred.port {
						return true
					}
				}
				return false
			})
			if len(n.natsConfigs) == 0 {
				return
			}
		}
	}
}

func (n *NatsClient) liveConnections() []*natsConnectionDescriptor {
	n.connectionsMutex.RLock()
	defer n.connectionsMutex.RUnlock()

	retVal := make([]*natsConnectionDescriptor, 0, len(n.connections))
	for _, desc := range n.connections {
		if desc.isLive {
			retVal = append(retVal, desc)
		}
	}
	return retVal
}

func (n *NatsClient) onConnect(c *nats.Conn) {
	n.connectionsMutex.Lock()
	defer n.connectionsMutex.Unlock()
	descr, ok := n.connections[c]
	if ok {
		descr.isLive = true
	}
}

func (n *NatsClient) onDisconnect(c *nats.Conn, err error) {
	n.connectionsMutex.Lock()
	defer n.connectionsMutex.Unlock()
	descr, ok := n.connections[c]
	if ok {
		descr.isLive = false
	}
}

func (n *NatsClient) natsConnect(cred natsConfig) (*natsConnectionDescriptor, error) {
	connDesc := &natsConnectionDescriptor{
		isLive:           true,
		rpcSubscriptions: []*nats.Subscription{},
	}

	natsUrl := fmt.Sprintf("nats://%s:%d", cred.host, cred.port)
	nc, err := nats.Connect(natsUrl, cred.options, func(o *nats.Options) error {
		o.ConnectedCB = n.onConnect
		o.ReconnectedCB = n.onConnect
		o.DisconnectedErrCB = n.onDisconnect
		return nil
	})
	if err != nil {
		return nil, err
	}
	connDesc.conn = nc

	for _, subsCred := range n.rpcConfigs {
		subscription, err := nc.QueueSubscribe(subsCred.subj, subsCred.queue, subsCred.fn)
		if err != nil {
			nc.Close()
			return nil, err
		}
		subscription.SetPendingLimits(
			8, //nats.DefaultSubPendingMsgsLimit,
			nats.DefaultSubPendingBytesLimit,
		)
		connDesc.rpcSubscriptions = append(connDesc.rpcSubscriptions, subscription)
	}

	return connDesc, nil
}
