package pubsub_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/moby/pubsub"
)

func TestSendToOneSub(t *testing.T) {
	p := pubsub.NewPublisher(100*time.Millisecond, 10)
	t.Cleanup(p.Close)
	c := p.Subscribe()

	p.Publish("hi")

	select {
	case msg := <-c:
		if msg.(string) != "hi" {
			t.Fatalf("expected message hi but received %v", msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestSendToMultipleSubs(t *testing.T) {
	p := pubsub.NewPublisher(100*time.Millisecond, 10)
	t.Cleanup(p.Close)

	subs := []chan any{p.Subscribe(), p.Subscribe(), p.Subscribe()}

	p.Publish("hi")

	for _, c := range subs {
		select {
		case msg := <-c:
			if msg.(string) != "hi" {
				t.Fatalf("expected message hi but received %v", msg)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for message")
		}
	}
}

func TestEvictOneSub(t *testing.T) {
	p := pubsub.NewPublisher(100*time.Millisecond, 10)
	t.Cleanup(p.Close)

	s1 := p.Subscribe()
	s2 := p.Subscribe()

	p.Evict(s1)
	p.Publish("hi")

	select {
	case _, ok := <-s1:
		if ok {
			t.Fatal("expected s1 to be closed after eviction")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for s1 to close after eviction")
	}

	select {
	case msg := <-s2:
		if msg.(string) != "hi" {
			t.Fatalf("expected message hi but received %v", msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for s2 to receive message")
	}
}

func TestClosePublisher(t *testing.T) {
	p := pubsub.NewPublisher(100*time.Millisecond, 10)
	subs := []chan any{p.Subscribe(), p.Subscribe(), p.Subscribe()}

	p.Close()

	for _, c := range subs {
		select {
		case _, ok := <-c:
			if ok {
				t.Fatal("expected all subscriber channels to be closed")
			}
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for subscriber channel to close")
		}
	}
}

const sampleText = "test"

type testSubscriber struct {
	dataCh chan any
	ch     chan error
}

func (s *testSubscriber) Wait() error {
	return <-s.ch
}

func newTestSubscriber(p *pubsub.Publisher) *testSubscriber {
	ts := &testSubscriber{
		dataCh: p.Subscribe(),
		ch:     make(chan error, 1), // prevent deadlock if we produce an error before Wait()
	}
	go func() {
		defer close(ts.ch)
		for data := range ts.dataCh {
			s, ok := data.(string)
			if !ok {
				ts.ch <- fmt.Errorf("unexpected type %T", data)
				return
			}
			if s != sampleText {
				ts.ch <- fmt.Errorf("unexpected text %q", s)
				return
			}
		}
		ts.ch <- nil
	}()
	return ts
}

// for testing with -race
func TestPubSubRace(t *testing.T) {
	p := pubsub.NewPublisher(0, 1024)
	t.Cleanup(p.Close)

	subs := make([]*testSubscriber, 0, 50)
	for j := 0; j < 50; j++ {
		subs = append(subs, newTestSubscriber(p))
	}

	done := make(chan struct{})
	go func() {
		for j := 0; j < 1000; j++ {
			p.Publish(sampleText)
		}
		close(done)
	}()

	// Evict while publishes are running.
	for _, s := range subs {
		p.Evict(s.dataCh)
	}

	<-done

	for _, s := range subs {
		if err := s.Wait(); err != nil {
			t.Fatal(err)
		}
	}
}
