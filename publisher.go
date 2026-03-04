package pubsub

import (
	"sync"
	"time"
)

var wgPool = sync.Pool{New: func() interface{} { return new(sync.WaitGroup) }}

// NewPublisher creates a new pub/sub publisher to broadcast messages.
// The duration is used as the send timeout as to not block the publisher publishing
// messages to other clients if one client is slow or unresponsive.
// The buffer is used when creating new channels for subscribers.
func NewPublisher(publishTimeout time.Duration, buffer int) *Publisher {
	return &Publisher{
		buffer:      buffer,
		timeout:     publishTimeout,
		subscribers: make(map[subscriber]topicFunc),
	}
}

type subscriber chan interface{}
type topicFunc func(v interface{}) bool

// Publisher is basic pub/sub structure. Allows to send events and subscribe
// to them. Can be safely used from multiple goroutines.
type Publisher struct {
	m           sync.RWMutex
	buffer      int
	timeout     time.Duration
	subscribers map[subscriber]topicFunc
}

// Len returns the number of subscribers for the publisher
func (p *Publisher) Len() int {
	p.m.RLock()
	i := len(p.subscribers)
	p.m.RUnlock()
	return i
}

// Subscribe adds a new subscriber to the publisher returning the channel.
func (p *Publisher) Subscribe() chan interface{} {
	return p.SubscribeTopic(nil)
}

// SubscribeTopic adds a new subscriber that filters messages sent by a topic.
func (p *Publisher) SubscribeTopic(topic topicFunc) chan interface{} {
	ch := make(chan interface{}, p.buffer)
	p.m.Lock()
	p.subscribers[ch] = topic
	p.m.Unlock()
	return ch
}

// SubscribeTopicWithBuffer adds a new subscriber that filters messages sent by a topic.
// The returned channel has a buffer of the specified size.
func (p *Publisher) SubscribeTopicWithBuffer(topic topicFunc, buffer int) chan interface{} {
	ch := make(chan interface{}, buffer)
	p.m.Lock()
	p.subscribers[ch] = topic
	p.m.Unlock()
	return ch
}

// Evict removes the specified subscriber from receiving any more messages.
func (p *Publisher) Evict(sub chan interface{}) {
	p.m.Lock()
	_, exists := p.subscribers[sub]
	if exists {
		delete(p.subscribers, sub)
		close(sub)
	}
	p.m.Unlock()
}

// Publish sends the data in v to all subscribers currently registered with the publisher.
func (p *Publisher) Publish(v interface{}) {
	p.m.RLock()
	if len(p.subscribers) == 0 {
		p.m.RUnlock()
		return
	}

	var t *time.Timer
	var done chan struct{}
	if p.timeout > 0 {
		done = make(chan struct{})
		t = time.AfterFunc(p.timeout, func() {
			close(done) // cancel all goroutines
		})
	}

	wg := wgPool.Get().(*sync.WaitGroup)
	for sub, topic := range p.subscribers {
		wg.Add(1)
		go sendTopic(sub, topic, v, done, wg)
	}
	wg.Wait()
	p.m.RUnlock()
	wgPool.Put(wg)
	if t != nil {
		t.Stop()
	}
}

// Close closes the channels to all subscribers registered with the publisher.
func (p *Publisher) Close() {
	p.m.Lock()
	for sub := range p.subscribers {
		delete(p.subscribers, sub)
		close(sub)
	}
	p.m.Unlock()
}

func sendTopic(sub subscriber, topic topicFunc, v interface{}, done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	if topic != nil && !topic(v) {
		return
	}

	if done != nil {
		select {
		case sub <- v:
		case <-done:
		}
		return
	}

	select {
	case sub <- v:
	default:
	}
}
