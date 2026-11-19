package app

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dnstapir/tapir-analyse-lib/common"
	"github.com/dnstapir/tapir-analyse-lib/libtapir"
	"github.com/nats-io/nats.go/jetstream"
)

const c_N_HANDLERS = 3
const c_NATS_DELIM = common.NATS_DELIM

type Conf struct {
	Debug       bool     `toml:"debug"`
	Interval    int      `toml:"interval"`
	Tags        []string `toml:"tags"`
	Observation string   `toml:"observation"`
	AllowCustom bool     `toml:"allow_custom"`
	DryRun      bool     `toml:"dry_run"`
	AnalystID   string
	Log         common.Logger
	NatsHandle  nats
}

type appHandle struct {
	id          string
	observation string
	log         common.Logger
	natsHandle  nats
	ticker      *time.Ticker
	exitCh      chan<- common.Exit
	tags        []string
	dryRun      bool
	pm

	kvMu     sync.RWMutex
	kvState  map[string][]byte
	kvSynced atomic.Bool
}

type pm struct {
}

type job struct {
	isTick   bool
	tickData int64
	msg      common.NatsMsg
}

type nats interface {
	ActivateSubscription(context.Context) (<-chan common.NatsMsg, error)
	SetObservation(context.Context, string, string) error
	Shutdown() error
	WatchTags(context.Context, []string) (<-chan jetstream.KeyValueEntry, error)
	ListTagKeys(context.Context, string) ([]string, error)
	TagKey(string, string) string
	ParseTagKey(string) (string, bool)
}

func validateTags(tags []string, allowCustom bool) ([]string, error) {
	wellDefined := common.TagNames()
	clean := make([]string, 0, len(tags))
	var custom []string

	for _, t := range tags {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if !slices.Contains(wellDefined, t) {
			custom = append(custom, t)
		}
		clean = append(clean, t)
	}

	if len(custom) > 0 && !allowCustom {
		return nil, fmt.Errorf("custom tags %v not allowed (allow_custom is false): %w",
			custom, common.ErrBadParam)
	}

	return clean, nil
}

func Create(conf Conf) (*appHandle, error) {
	a := new(appHandle)

	if conf.Log == nil {
		return nil, common.ErrBadHandle
	}
	a.log = conf.Log

	if conf.NatsHandle == nil {
		return nil, common.ErrBadHandle
	}
	a.natsHandle = conf.NatsHandle

	if conf.AnalystID == "" {
		a.log.Error("Bad analyst ID during creation")
		return nil, common.ErrBadParam
	}
	a.id = conf.AnalystID

	if conf.Interval > 0 {
		a.ticker = time.NewTicker(time.Duration(conf.Interval) * time.Second)
	} else {
		a.ticker = time.NewTicker(time.Duration(math.MaxInt32) * time.Second)
		a.ticker.Stop()
		a.log.Warning("No interval set. Won't refresh list.")
	}

	tags, err := validateTags(conf.Tags, conf.AllowCustom)
	if err != nil {
		a.log.Error("Invalid tag configuration: %s", err)
		return nil, err
	}
	a.tags = tags

	if _, ok := common.OBS_MAP[conf.Observation]; !ok {
		a.log.Error("Unknown observation in config: '%s'", conf.Observation)
		return nil, common.ErrBadParam
	}
	a.observation = conf.Observation

	a.dryRun = conf.DryRun
	a.kvState = make(map[string][]byte)

	a.log.Debug("Main app debug logging enabled")
	return a, nil
}

func (a *appHandle) Run(ctx context.Context, exitCh chan<- common.Exit) {
	defer a.ticker.Stop()

	var natsChan <-chan common.NatsMsg
	var err error
	a.exitCh = exitCh
	jobChan := make(chan job, 10)

	natsChan, err = a.natsHandle.ActivateSubscription(ctx)
	if err != nil {
		a.log.Error("Couldn't activate NATS subscription: '%s'", err)
		a.exitCh <- common.Exit{ID: a.id, Err: err}
		return
	}

	kvChan, err := a.natsHandle.WatchTags(ctx, a.tags)
	if err != nil {
		a.log.Error("Couldn't activate tag watcher: '%s'", err)
		a.exitCh <- common.Exit{ID: a.id, Err: err}
		return
	}

	var wg sync.WaitGroup
	for range c_N_HANDLERS {
		wg.Go(func() {
			for j := range jobChan {
				a.handleJob(ctx, j)
			}
			a.log.Info("Worker done!")
		})
	}

	wg.Go(func() {
		a.watchLoop(ctx, kvChan)
		a.log.Info("KV watcher loop done!")
	})

MAIN_APP_LOOP:
	for {
		select {
		case t := <-a.ticker.C:
			a.log.Debug("Tick")
			j := job{
				isTick:   true,
				tickData: t.Unix(),
			}
			select {
			case jobChan <- j:
			case <-ctx.Done():
				break MAIN_APP_LOOP
			}
		case natsMsg, ok := <-natsChan:
			if !ok {
				a.log.Warning("NATS channel closed")
				natsChan = nil
			} else {
				a.log.Debug("Incoming NATS message")
				j := job{
					msg: natsMsg,
				}
				select {
				case jobChan <- j:
				case <-ctx.Done():
					break MAIN_APP_LOOP
				}
			}
		case <-ctx.Done():
			a.log.Info("Stopping main worker thread")
			break MAIN_APP_LOOP
		}
	}

	close(jobChan)

	wg.Wait()

	err = a.natsHandle.Shutdown()
	if err != nil {
		a.log.Error("Encountered '%s' during NATS shutdown", err)
	}

	a.exitCh <- common.Exit{ID: a.id, Err: err}
	a.log.Info("Main app shutdown done")
	return
}

func (a *appHandle) handleJob(ctx context.Context, j job) {
	if j.isTick {
		a.handleTick(ctx, j.tickData)
	}
}

func (a *appHandle) handleTick(ctx context.Context, epoch int64) {
	a.log.Debug("Received tick '%d'", epoch)
	if epoch == 0 {
		a.log.Debug("Tick had zero value, probably garbage. Won't handle...")
		return
	}

	//panic("not impl")
}

func (a *appHandle) watchLoop(ctx context.Context, kvChan <-chan jetstream.KeyValueEntry) {
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-kvChan:
			if !ok {
				a.log.Warning("KV watcher channel closed")
				return
			}
			if entry == nil {
				// Initial replay finished; state is now consistent with the bucket
				a.kvSynced.Store(true)
				a.log.Info("KV initial sync done")
				continue
			}
			a.handleKV(ctx, entry)
		}
	}
}

func (a *appHandle) handleKV(ctx context.Context, e jetstream.KeyValueEntry) {
	a.log.Debug("KV %s key=%s rev=%d", e.Operation(), e.Key(), e.Revision())

	if len(a.tags) == 0 {
		a.log.Warning("No tags configured, nothing to match")
		return
	}

	domain, ok := a.natsHandle.ParseTagKey(e.Key())
	if !ok {
		a.log.Warning("Could not extract domain from tag key '%s'", e.Key())
		return
	}

	parts := []string{"*", "*", libtapir.FlipDomainName(domain)}
	filter := common.NormalizeNatsSubject(strings.Join(parts, common.NATS_DELIM))

	lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	tagKeys, err := a.natsHandle.ListTagKeys(lctx, filter)
	if err != nil {
		a.log.Error("Could not list tag keys for '%s': %s", domain, err)
		return
	}

	for _, t := range a.tags {
		if !slices.Contains(tagKeys, a.natsHandle.TagKey(t, domain)) {
			return // tag(s) missing, not setting observation
		}
	}

	if a.dryRun {
		a.log.Info("DRY RUN: would set observation '%s' for '%s'", a.observation, domain)
		return
	}

	if err := a.natsHandle.SetObservation(ctx, domain, a.observation); err != nil {
		a.log.Error("Could not set observation '%s' for '%s': %s", a.observation, domain, err)
		return
	}
	a.log.Debug("Set observation '%s' for '%s'", a.observation, domain)

}
