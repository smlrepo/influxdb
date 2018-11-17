package control

import (
	"context"

	_ "github.com/influxdata/flux/builtin"
	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/platform"
	"go.uber.org/zap"
)

func NewController(s storage.Store, logger *zap.Logger) *control.Controller {
	// flux
	var (
		concurrencyQuota = 10
		memoryBytesQuota = 1e6
	)

	cc := control.Config{
		ExecutorDependencies: make(execute.Dependencies),
		ConcurrencyQuota:     concurrencyQuota,
		MemoryBytesQuota:     int64(memoryBytesQuota),
		Logger:               logger,
		Verbose:              false,
	}

	return control.New(cc)
}

type orgLookup struct{}

func mustIDFromString(name string) platform.ID {
	id, err := platform.IDFromString(name)
	if err != nil {
		panic(err)
	}
	return *id
}

func (l orgLookup) Lookup(ctx context.Context, name string) (platform.ID, bool) {
	return mustIDFromString(name), true
}

type bucketLookup struct{}

func (l bucketLookup) Lookup(orgID platform.ID, name string) (platform.ID, bool) {
	return mustIDFromString(name), true
}
