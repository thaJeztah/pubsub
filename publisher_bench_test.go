package pubsub_test

import (
	"testing"
	"time"

	"github.com/moby/pubsub"
)

const publishTimeout = 100 * time.Millisecond

func startDrainer(ch chan any) (stop func()) {
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
			}
		}
	}()
	return func() { close(done) }
}

func startDrainers(chs []chan any) (stop func()) {
	stops := make([]func(), len(chs))
	for i, ch := range chs {
		stops[i] = startDrainer(ch)
	}
	return func() {
		for _, s := range stops {
			s()
		}
	}
}

func benchmarkPublishNSubs(b *testing.B, n int, timeout time.Duration, buffer int, v any) {
	b.ReportAllocs()

	p := pubsub.NewPublisher(timeout, buffer)
	chs := make([]chan any, n)
	for i := 0; i < n; i++ {
		chs[i] = p.Subscribe()
	}
	stop := startDrainers(chs)
	defer func() {
		stop()
		for _, ch := range chs {
			p.Evict(ch)
		}
		p.Close()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Publish(v)
	}
}

func benchmarkPublishNSubsTopic(b *testing.B, n int, timeout time.Duration, buffer int, match func(any) bool, v any) {
	b.ReportAllocs()

	p := pubsub.NewPublisher(timeout, buffer)
	chs := make([]chan any, n)
	for i := 0; i < n; i++ {
		chs[i] = p.SubscribeTopic(match)
	}
	stop := startDrainers(chs)
	defer func() {
		stop()
		for _, ch := range chs {
			p.Evict(ch)
		}
		p.Close()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Publish(v)
	}
}

func BenchmarkPublish_NoSubscribers(b *testing.B) {
	b.ReportAllocs()

	p := pubsub.NewPublisher(0, 0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p.Publish("x")
	}
}

func BenchmarkPublish_1Subscriber(b *testing.B) {
	b.ReportAllocs()

	p := pubsub.NewPublisher(0, 0)
	ch := p.Subscribe()
	stop := startDrainer(ch)
	defer func() {
		stop()
		p.Evict(ch)
		p.Close()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Publish("x")
	}
}

func BenchmarkPublish_WithTimeout_1Subscriber(b *testing.B) {
	b.ReportAllocs()

	p := pubsub.NewPublisher(publishTimeout, 0)
	ch := p.Subscribe()
	stop := startDrainer(ch)
	defer func() {
		stop()
		p.Evict(ch)
		p.Close()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Publish("x")
	}
}

func BenchmarkPublish_10Subscribers(b *testing.B)   { benchmarkPublishNSubs(b, 10, 0, 0, "x") }
func BenchmarkPublish_100Subscribers(b *testing.B)  { benchmarkPublishNSubs(b, 100, 0, 0, "x") }
func BenchmarkPublish_1000Subscribers(b *testing.B) { benchmarkPublishNSubs(b, 1000, 0, 0, "x") }

func BenchmarkPublish_WithTimeout_10Subscribers(b *testing.B) {
	benchmarkPublishNSubs(b, 10, publishTimeout, 0, "x")
}
func BenchmarkPublish_WithTimeout_100Subscribers(b *testing.B) {
	benchmarkPublishNSubs(b, 100, publishTimeout, 0, "x")
}
func BenchmarkPublish_WithTimeout_1000Subscribers(b *testing.B) {
	benchmarkPublishNSubs(b, 1000, publishTimeout, 0, "x")
}

func BenchmarkPublish_50Subscribers(b *testing.B) {
	benchmarkPublishNSubs(b, 50, 0, 1024, "test")
}

func BenchmarkPublish_WithTimeout_50Subscribers(b *testing.B) {
	benchmarkPublishNSubs(b, 50, publishTimeout, 1024, "test")
}

func BenchmarkSubscribe(b *testing.B) {
	b.ReportAllocs()

	p := pubsub.NewPublisher(0, 0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ch := p.Subscribe()
		p.Evict(ch)
	}
}

func BenchmarkEvict(b *testing.B) {
	b.ReportAllocs()

	p := pubsub.NewPublisher(0, 0)
	chs := make([]chan any, b.N)
	for i := 0; i < b.N; i++ {
		chs[i] = p.Subscribe()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Evict(chs[i])
	}
}

func BenchmarkClose_1000Subscribers(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		p := pubsub.NewPublisher(0, 0)
		for j := 0; j < 1000; j++ {
			_ = p.Subscribe()
		}
		b.StartTimer()
		p.Close()
		b.StopTimer()
	}
}

func BenchmarkSubscribe_UnderPublishLoad(b *testing.B) {
	b.ReportAllocs()

	p := pubsub.NewPublisher(0, 0)
	ch := p.Subscribe()
	stopDrain := startDrainer(ch)
	defer func() {
		stopDrain()
		p.Evict(ch)
		p.Close()
	}()

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				p.Publish("x")
			}
		}
	}()
	defer close(stop)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub := p.Subscribe()
		p.Evict(sub)
	}
}

type statsLike struct {
	ID     string
	Name   string
	OSType string
	Read   time.Time

	CPU struct {
		TotalUsage uint64
		Percpu     []uint64
	}

	Memory struct {
		Usage uint64
		Stats map[string]uint64
	}

	Networks map[string]struct {
		RxBytes uint64
		TxBytes uint64
	}
}

var sink any

func makeStatsLike() statsLike {
	return statsLike{
		ID:     "0123456789abcdef",
		Name:   "ctr",
		OSType: "linux",
		Read:   time.Unix(0, 0),
		CPU: struct {
			TotalUsage uint64
			Percpu     []uint64
		}{TotalUsage: 123, Percpu: []uint64{1, 2, 3, 4}},
		Memory: struct {
			Usage uint64
			Stats map[string]uint64
		}{Usage: 456, Stats: map[string]uint64{"cache": 1, "rss": 2}},
		Networks: map[string]struct {
			RxBytes uint64
			TxBytes uint64
		}{"eth0": {RxBytes: 1, TxBytes: 2}},
	}
}

func BenchmarkPublish_50Subscribers_StatsLike_Value(b *testing.B) {
	v := makeStatsLike()
	benchmarkPublishNSubs(b, 50, 0, 1024, v)
	sink = v
}

func BenchmarkPublish_WithTimeout_50Subscribers_StatsLike_Value(b *testing.B) {
	v := makeStatsLike()
	benchmarkPublishNSubs(b, 50, publishTimeout, 1024, v)
	sink = v
}

func BenchmarkPublish_50Subscribers_StatsLike_Pointer(b *testing.B) {
	v := makeStatsLike()
	benchmarkPublishNSubs(b, 50, 0, 1024, &v)
	sink = &v
}

func BenchmarkPublish_WithTimeout_50Subscribers_StatsLike_Pointer(b *testing.B) {
	v := makeStatsLike()
	benchmarkPublishNSubs(b, 50, publishTimeout, 1024, &v)
	sink = &v
}

func BenchmarkSubscribeTopic(b *testing.B) {
	b.ReportAllocs()

	p := pubsub.NewPublisher(0, 0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ch := p.SubscribeTopic(func(any) bool { return true })
		p.Evict(ch)
	}
}

func BenchmarkPublish_50Subscribers_StatsLike_TopicAssertValue(b *testing.B) {
	match := func(m any) bool {
		v := m.(statsLike)
		sink = v
		return true
	}
	v := makeStatsLike()
	benchmarkPublishNSubsTopic(b, 50, 0, 1024, match, v)
}

func BenchmarkPublish_WithTimeout_50Subscribers_StatsLike_TopicAssertValue(b *testing.B) {
	match := func(m any) bool {
		v := m.(statsLike)
		sink = v
		return true
	}
	v := makeStatsLike()
	benchmarkPublishNSubsTopic(b, 50, publishTimeout, 1024, match, v)
}

func BenchmarkPublish_50Subscribers_StatsLike_TopicAssertPointer(b *testing.B) {
	match := func(m any) bool {
		v := m.(*statsLike)
		sink = v
		return true
	}
	v := makeStatsLike()
	benchmarkPublishNSubsTopic(b, 50, 0, 1024, match, &v)
}

func BenchmarkPublish_WithTimeout_50Subscribers_StatsLike_TopicAssertPointer(b *testing.B) {
	match := func(m any) bool {
		v := m.(*statsLike)
		sink = v
		return true
	}
	v := makeStatsLike()
	benchmarkPublishNSubsTopic(b, 50, publishTimeout, 1024, match, &v)
}
