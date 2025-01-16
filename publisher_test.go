package pubsub

import (
	"fmt"
	"testing"
	"time"
)

func TestSendToOneSub(t *testing.T) {
	p := NewPublisher[string](100*time.Millisecond, 10)
	c := p.Subscribe()

	p.Publish("hi")

	msg := <-c
	if msg != "hi" {
		t.Fatalf("expected message hi but received %v", msg)
	}
}

func TestSendToMultipleSubs(t *testing.T) {
	p := NewPublisher[string](100*time.Millisecond, 10)
	var subs []chan string
	subs = append(subs, p.Subscribe(), p.Subscribe(), p.Subscribe())

	p.Publish("hi")

	for _, c := range subs {
		msg := <-c
		if msg != "hi" {
			t.Fatalf("expected message hi but received %v", msg)
		}
	}
}

func TestSendToMultipleSubsInt(t *testing.T) {
	p := NewPublisher[int](100*time.Millisecond, 10)
	var subs []chan int
	subs = append(subs, p.Subscribe(), p.Subscribe(), p.Subscribe())

	p.Publish(123)

	for _, c := range subs {
		msg := <-c
		if msg != 123 {
			t.Fatalf("expected message 123 but received %v", msg)
		}
	}
}

func TestEvictOneSub(t *testing.T) {
	p := NewPublisher[string](100*time.Millisecond, 10)
	s1 := p.Subscribe()
	s2 := p.Subscribe()

	p.Evict(s1)
	p.Publish("hi")
	if _, ok := <-s1; ok {
		t.Fatal("expected s1 to not receive the published message")
	}

	msg := <-s2
	if msg != "hi" {
		t.Fatalf("expected message hi but received %v", msg)
	}
}

func TestClosePublisher(t *testing.T) {
	p := NewPublisher[string](100*time.Millisecond, 10)
	var subs []chan string
	subs = append(subs, p.Subscribe(), p.Subscribe(), p.Subscribe())
	p.Close()

	for _, c := range subs {
		if _, ok := <-c; ok {
			t.Fatal("expected all subscriber channels to be closed")
		}
	}
}

const sampleText = "test"

type testSubscriber[T any] struct {
	dataCh chan T
	ch     chan error
}

func (s *testSubscriber[T]) Wait() error {
	return <-s.ch
}

func newTestSubscriber(p *Publisher[string]) *testSubscriber[string] {
	ts := &testSubscriber[string]{
		dataCh: p.Subscribe(),
		ch:     make(chan error),
	}
	go func() {
		for data := range ts.dataCh {
			if data != sampleText {
				ts.ch <- fmt.Errorf("unexpected text %s", data)
				break
			}
		}
		close(ts.ch)
	}()
	return ts
}

// for testing with -race
func TestPubSubRace(t *testing.T) {
	p := NewPublisher[string](0, 1024)
	var subs []*testSubscriber[string]
	for j := 0; j < 50; j++ {
		subs = append(subs, newTestSubscriber(p))
	}
	for j := 0; j < 1000; j++ {
		p.Publish(sampleText)
	}
	time.AfterFunc(1*time.Second, func() {
		for _, s := range subs {
			p.Evict(s.dataCh)
		}
	})
	for _, s := range subs {
		_ = s.Wait()
	}
}

func BenchmarkPubSub(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		p := NewPublisher[string](0, 1024)
		var subs []*testSubscriber[string]
		for j := 0; j < 50; j++ {
			subs = append(subs, newTestSubscriber(p))
		}
		b.StartTimer()
		for j := 0; j < 1000; j++ {
			p.Publish(sampleText)
		}
		time.AfterFunc(1*time.Second, func() {
			for _, s := range subs {
				p.Evict(s.dataCh)
			}
		})
		for _, s := range subs {
			if err := s.Wait(); err != nil {
				b.Fatal(err)
			}
		}
	}
}
