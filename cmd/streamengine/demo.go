package main

import (
	"log"
	"time"

	"streamengine/internal/source"
	"streamengine/internal/topology"
)

// runDemoFeed drives a deterministic workload through every engine component:
// out-of-order events, watermark advances, window triggers, session expiry,
// checkpoints, topology pause/resume and partition resizing.
func runDemoFeed(pipeline *topology.Pipeline, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	generator := source.NewGenerator(source.GeneratorSpec{
		Keys:       []string{"checkout", "cart", "search", "detail", "payment"},
		StartTime:  1000,
		StepTime:   60,
		JitterSpan: 600,
		PingEvery:  17,
	})
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	round := 0
	var offset int64
	var lastTS int64
	for {
		select {
		case <-stop:
			pipeline.Source.Stop()
			pipeline.Advance(lastTS + 10000)
			return
		case <-tick.C:
			round++
			ev := generator.Next()
			offset = ev.Offset + 1
			lastTS = ev.Timestamp
			pipeline.Source.Emit(ev)
			// Advance the watermark every few events so windows fire.
			if round%3 == 0 {
				advanceTo := ev.Timestamp + 300
				pipeline.Advance(advanceTo)
				lastTS = advanceTo
			}
			if round%17 == 0 {
				pipeline.RunCheckpoint(offset)
			}
			if round%29 == 0 {
				pipeline.Topology.Pause()
				pipeline.Topology.ApplyNow(ev)
			}
			if round%29 == 7 {
				pipeline.Topology.Resume()
			}
			if round%41 == 0 {
				next := 2
				if pipeline.Router.Partitions() == 2 {
					next = 4
				}
				pipeline.Router.Resize(next)
			}
			if round%37 == 0 {
				retryFrom := offset - 3
				_ = pipeline.RetryCheckpoint(retryFrom, func(from int64) error {
					generator.ReplayFrom(from)
					return nil
				})
				generator.SkipTo(offset)
			}
			if ts, idle := pipeline.Idle.ShouldAdvance(lastTS + 4000); idle {
				pipeline.Advance(ts)
			}
			if round%500 == 0 {
				checkoutValue, _ := pipeline.State.Get("checkout")
				log.Printf(
					"demo progress: offset=%d round=%d emitted=%d keys=%d checkout=%d",
					offset,
					round,
					generator.Emitted(),
					len(generator.Keys()),
					checkoutValue,
				)
			}
		}
	}
}
