package pubsub

import (
	"sync"
	"time"
)

var wgPool = sync.Pool{New: func() any { return new(sync.WaitGroup) }}

// NewPublisher creates a new Publisher.
//
// publishTimeout specifies the maximum amount of time a Publish call will
// wait when delivering a message to an individual subscriber. If the timeout
// expires, delivery to that subscriber is abandoned for that message.
//
// A zero publishTimeout means sends will not wait; delivery is attempted
// without blocking and the message may be dropped if the subscriber channel
// is not ready.
//
// buffer specifies the buffer size of channels created by Subscribe.
func NewPublisher(publishTimeout time.Duration, buffer int) *Publisher {
	return &Publisher{
		buffer:      buffer,
		timeout:     publishTimeout,
		subscribers: make(map[subscriber]func(v any) bool),
	}
}

type subscriber chan any

// Publisher implements a simple in-memory pub/sub broadcaster.
//
// A Publisher may be used concurrently from multiple goroutines.
//
// Messages are delivered to all current subscribers. Delivery is best-effort:
// depending on the configured timeout and channel buffering, messages may be
// dropped for slow subscribers.
type Publisher struct {
	m           sync.RWMutex
	buffer      int
	timeout     time.Duration
	subscribers map[subscriber]func(any) bool
}

// Len returns the number of subscribers for the publisher
func (p *Publisher) Len() int {
	p.m.RLock()
	defer p.m.RUnlock()
	return len(p.subscribers)
}

// Subscribe registers a new subscriber that receives all published messages.
// The returned channel has the default buffer size configured for the Publisher.
func (p *Publisher) Subscribe() chan any {
	return p.SubscribeTopic(nil)
}

// SubscribeTopic registers a new subscriber that only receives messages
// for which match returns true.
//
// The match function acts as a per-subscriber filter and is invoked
// synchronously by Publish for each subscriber. It must be fast and
// non-blocking: a slow or blocking match will delay Publish from returning
// and will keep the Publisher's read lock held longer, and may delay
// starting delivery to other subscribers in the same Publish call.
//
// If match is nil, all messages are delivered.
func (p *Publisher) SubscribeTopic(match func(any) bool) chan any {
	ch := make(chan any, p.buffer)
	p.m.Lock()
	p.subscribers[ch] = match
	p.m.Unlock()
	return ch
}

// SubscribeTopicWithBuffer registers a new subscriber with a custom
// channel buffer size. The match function has the same semantics as
// SubscribeTopic.
func (p *Publisher) SubscribeTopicWithBuffer(match func(any) bool, buffer int) chan any {
	ch := make(chan any, buffer)
	p.m.Lock()
	p.subscribers[ch] = match
	p.m.Unlock()
	return ch
}

// Evict removes sub from the Publisher and closes the channel.
// After Evict returns, sub will receive no further messages.
func (p *Publisher) Evict(sub chan any) {
	p.m.Lock()
	if _, exists := p.subscribers[sub]; exists {
		delete(p.subscribers, sub)
		close(sub)
	}
	p.m.Unlock()
}

// Publish broadcasts v to all currently registered subscribers.
//
// For each subscriber, match (if present) is evaluated first. If match
// returns false, the message is not delivered to that subscriber.
//
// Delivery to each subscriber is attempted in a separate goroutine.
// Publish waits for all delivery attempts to complete before returning.
//
// If publishTimeout was configured, delivery to an individual subscriber
// will block for up to that timeout while attempting to send. If the
// timeout expires before the subscriber channel is ready, the message
// is dropped.
//
// If publishTimeout is zero, delivery is attempted in a non-blocking
// manner and is dropped immediately if the subscriber channel is not ready.
//
// For large message values, publishing pointers instead of values can
// significantly reduce copying overhead, particularly when match performs
// type assertions.
func (p *Publisher) Publish(v any) {
	p.m.RLock()
	if len(p.subscribers) == 0 {
		p.m.RUnlock()
		return
	}

	wg := wgPool.Get().(*sync.WaitGroup)
	for sub, topic := range p.subscribers {
		wg.Add(1)
		go p.sendTopic(sub, topic, v, wg)
	}
	wg.Wait()
	wgPool.Put(wg)
	p.m.RUnlock()
}

// Close removes all subscribers and closes their channels.
// After Close, the Publisher has no subscribers.
func (p *Publisher) Close() {
	p.m.Lock()
	for sub := range p.subscribers {
		delete(p.subscribers, sub)
		close(sub)
	}
	p.m.Unlock()
}

func (p *Publisher) sendTopic(sub subscriber, match func(any) bool, v any, wg *sync.WaitGroup) {
	defer wg.Done()
	if match != nil && !match(v) {
		return
	}

	// send under a select as to not block if the receiver is unavailable
	if p.timeout > 0 {
		timeout := time.NewTimer(p.timeout)
		defer timeout.Stop()

		select {
		case sub <- v:
		case <-timeout.C:
		}
		return
	}

	select {
	case sub <- v:
	default:
	}
}
