// Package retention provides the retention policy enforcement service.
package retention // import "github.com/influxdata/influxdb/services/retention"

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// Service represents the retention policy enforcement service.
type Service struct {
	MetaClient interface {
		Databases() []meta.DatabaseInfo
		DeleteShardGroup(database, policy string, id uint64) error
		PruneShardGroups() error
	}
	TSDBStore interface {
		ShardIDs() []uint64
		DeleteShard(shardID uint64) error
	}

	config Config
	wg     sync.WaitGroup
	done   chan struct{}

	logger *zap.Logger
}

// NewService returns a configured retention policy enforcement service.
func NewService(c Config) *Service {
	return &Service{
		config: c,
		logger: zap.NewNop(),
	}
}

// Open starts retention policy enforcement.
func (s *Service) Open() error {
	if !s.config.Enabled || s.done != nil {
		return nil
	}

	s.logger.Info("Starting retention policy enforcement service",
		logger.DurationLiteral("check_interval", time.Duration(s.config.CheckInterval)))
	s.done = make(chan struct{})

	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.run() }()
	return nil
}

// Close stops retention policy enforcement.
func (s *Service) Close() error {
	if !s.config.Enabled || s.done == nil {
		return nil
	}

	s.logger.Info("Closing retention policy enforcement service")
	close(s.done)

	s.wg.Wait()
	s.done = nil
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.logger = log.With(zap.String("service", "retention"))
}

func (s *Service) run() {
	ticker := time.NewTicker(time.Duration(s.config.CheckInterval))
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return

		case <-ticker.C:
			log, logEnd := logger.NewOperation(s.logger, "Retention policy deletion check", "retention_delete_check")

			type deletionInfo struct {
				db string
				rp string
			}
			deletedShardIDs := make(map[uint64]deletionInfo)

			// Mark down if an error occurred during this function so we can inform the
			// user that we will try again on the next interval.
			// Without the message, they may see the error message and assume they
			// have to do it manually.
			var retryNeeded bool
			dbs := s.MetaClient.Databases()

			buf, _ := json.Marshal(dbs)
			log.Info("metadata", zap.String("json", string(buf)))
			log.Info("meta databases", zap.Int("n", len(dbs)))

			for _, d := range dbs {
				log.Info("meta database", zap.String("name", d.Name), zap.Int("rps", len(d.RetentionPolicies)))

				for _, r := range d.RetentionPolicies {
					log.Info("meta policy", zap.String("name", r.Name), zap.Int("groups", len(r.ShardGroups)), zap.Int("expired-groups", len(r.ExpiredShardGroups(time.Now().UTC()))))
					for _, g := range r.ShardGroups {
						log.Info("meta group", zap.Uint64("id", g.ID))
					}

					for _, g := range r.ExpiredShardGroups(time.Now().UTC()) {
						log.Info("expired meta group", zap.Uint64("id", g.ID), zap.String("db", d.Name), zap.String("rp", r.Name))

						if err := s.MetaClient.DeleteShardGroup(d.Name, r.Name, g.ID); err != nil {
							log.Info("Failed to delete shard group",
								logger.Database(d.Name),
								logger.ShardGroup(g.ID),
								logger.RetentionPolicy(r.Name),
								zap.Error(err))
							retryNeeded = true
							continue
						}

						log.Info("Deleted shard group",
							logger.Database(d.Name),
							logger.ShardGroup(g.ID),
							logger.RetentionPolicy(r.Name))

						// Store all the shard IDs that may possibly need to be removed locally.
						log.Info("delete shards", zap.Int("n", len(g.Shards)))
						for _, sh := range g.Shards {
							log.Info("delete id", zap.Uint64("id", sh.ID))
							deletedShardIDs[sh.ID] = deletionInfo{db: d.Name, rp: r.Name}
						}
					}
				}
			}

			// Remove shards if we store them locally
			log.Info("local shard ids", zap.Uint64s("ids", s.TSDBStore.ShardIDs()))
			for _, id := range s.TSDBStore.ShardIDs() {
				log.Info("local shard ids", zap.Uint64("id", id))
				if info, ok := deletedShardIDs[id]; ok {
					log.Info("delete local shard", zap.Uint64("id", id))
					if err := s.TSDBStore.DeleteShard(id); err != nil {
						log.Info("Failed to delete shard",
							logger.Database(info.db),
							logger.Shard(id),
							logger.RetentionPolicy(info.rp),
							zap.Error(err))
						retryNeeded = true
						continue
					}
					log.Info("Deleted shard",
						logger.Database(info.db),
						logger.Shard(id),
						logger.RetentionPolicy(info.rp))
				}
			}
			log.Info("local shards done")

			if err := s.MetaClient.PruneShardGroups(); err != nil {
				log.Info("Problem pruning shard groups", zap.Error(err))
				retryNeeded = true
			}

			if retryNeeded {
				log.Info("One or more errors occurred during shard deletion and will be retried on the next check", logger.DurationLiteral("check_interval", time.Duration(s.config.CheckInterval)))
			}

			logEnd()
		}
	}
}
