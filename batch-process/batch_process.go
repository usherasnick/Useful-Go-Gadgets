package batchprocess

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	__DefaultBatcherNum      = 1
	__DefaultMaxBatchSize    = 16
	__DefaultFlushTimeMs     = 2000
	__DefaultSourceQueueSize = 32

	__DefaultBatcherPutTimeout   = 10 * time.Second
	__DefaultBatcherCloseTimeout = 5 * time.Second
)

// BatcherGroupCfg 匹处理器组配置
type BatcherGroupCfg struct {
	BatcherNum         int
	BatcherConcurrency int
	MaxBatchSize       int
	FlushTimeMs        int
	SourceQueueSize    int
}

// SourceItem 待处理的事务单元
type SourceItem struct {
	key  string
	item interface{}
}

// BatchItem 批处理队列中待处理的事务单元
type BatchItem struct {
	CreatedTime time.Time
	Items       []interface{}
	NextItemIdx int
}

// DoBatch 匹处理器处理事务的处理函数
type DoBatch func(*BatchItem) error

// Batcher 匹处理器, 用于批量处理指定的事务
type Batcher struct {
	mu sync.RWMutex

	id                 int
	cfg                *BatcherGroupCfg
	sourceQ            chan SourceItem
	batchQ             chan BatchItem
	doBatch            DoBatch
	flushInterval      time.Duration
	expiredInterval    time.Duration
	processedPerThread []int64
	done               chan struct{}
}

// BatcherGroup 一组匹处理器
type BatcherGroup []*Batcher

// NewBatcherGroup 返回BatcherGroup实例.
func NewBatcherGroup(cfg *BatcherGroupCfg) BatcherGroup {
	if cfg == nil {
		log.Error().Msg("no batcher")
		return nil
	}
	if cfg.BatcherNum == 0 {
		cfg.BatcherNum = __DefaultBatcherNum
	}
	if cfg.BatcherConcurrency == 0 {
		n := runtime.NumCPU()
		cfg.BatcherConcurrency = n
	}
	if cfg.MaxBatchSize == 0 {
		cfg.MaxBatchSize = __DefaultMaxBatchSize
	}
	if cfg.FlushTimeMs == 0 {
		cfg.FlushTimeMs = __DefaultFlushTimeMs
	}
	if cfg.SourceQueueSize == 0 {
		cfg.SourceQueueSize = __DefaultSourceQueueSize
	}

	bg := make(BatcherGroup, cfg.BatcherNum)
	for i := 0; i < cfg.BatcherNum; i++ {
		b := NewBatcher(i, cfg)
		bg[i] = b
	}
	return bg
}

// Start 开始运行所有的批处理器.
func (bg BatcherGroup) Start(doBatch DoBatch) {
	for _, b := range bg {
		b.Start(doBatch)
	}
}

// Close 停止运行所有的批处理器.
func (bg BatcherGroup) Close() {
	for _, b := range bg {
		b.Close()
	}
}

// Put 将待处理的事务加入批处理器组, 并按照关键词分发给指定的批处理器处理.
func (bg BatcherGroup) Put(key string, job interface{}) error {
	return bg[FNV1av32(key)%uint32(len(bg))].Put(key, job)
}

// Stat 显示所有批处理器的工作状态.
func (bg BatcherGroup) Stat() {
	var total int64
	log.Info().Msgf("/******************** Stat ********************/")
	for i, b := range bg {
		t := b.Stat()
		if t > 0 {
			log.Info().Msgf("[batcher-%d] processed %d batched requests", i, t)
			total += t
		}
	}
	if total > 0 {
		log.Info().Msgf("totally processed %d batched requests", total)
	}
	log.Info().Msgf("/******************** Stat ********************/")
}

// NewBatcher 返回Batcher实例.
func NewBatcher(id int, cfg *BatcherGroupCfg) *Batcher {
	return &Batcher{
		id:                 id,
		cfg:                cfg,
		sourceQ:            make(chan SourceItem, cfg.SourceQueueSize),
		batchQ:             make(chan BatchItem, cfg.BatcherConcurrency+1),
		flushInterval:      time.Duration(cfg.FlushTimeMs) * time.Millisecond,
		expiredInterval:    time.Duration(cfg.FlushTimeMs*9/10) * time.Millisecond,
		processedPerThread: make([]int64, cfg.BatcherConcurrency),
		done:               make(chan struct{}),
	}
}

// Start 开始运行批处理器.
func (b *Batcher) Start(doBatch DoBatch) {
	log.Info().Msgf("start batcher-%d", b.id)
	b.doBatch = doBatch
	go b.source()
	go b.sink()
}

// Close 停止运行批处理器.
func (b *Batcher) Close() {
	if b.sourceQ != nil {
		close(b.sourceQ)
		b.sourceQ = nil
	}

	select {
	case <-b.done:
		log.Info().Msgf("batcher-%d has done", b.id)
	case <-time.After(__DefaultBatcherCloseTimeout):
		log.Warn().Msgf("timeout to close batcher-%d", b.id)
	}
}

// Put 将待处理的事务加入批处理器.
func (b *Batcher) Put(key string, job interface{}) error {
	item := SourceItem{
		key:  key,
		item: job,
	}
	select {
	case b.sourceQ <- item:
	case <-time.After(__DefaultBatcherPutTimeout):
		log.Warn().Msgf("timeout to put item for batcher-%d", b.id)
		return fmt.Errorf("timeout to put item for batcher-%d", b.id)
	}
	return nil
}

func (b *Batcher) source() {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batchTable := make(map[uint32]BatchItem)
BATCHER_LOOP:
	for {
		select {
		case item, ok := <-b.sourceQ:
			if !ok {
				break BATCHER_LOOP
			}
			k := FNV1av32(item.key) % uint32(b.cfg.BatcherConcurrency)
			if _, exist := batchTable[k]; !exist {
				batchTable[k] = BatchItem{
					CreatedTime: time.Now(),
					Items:       make([]interface{}, b.cfg.MaxBatchSize),
					NextItemIdx: 0,
				}
			}
			b.appendItemWithFlushOp(batchTable, k, &item)
		case <-ticker.C:
			// b.flush(batchTable, false /* flush */)
		}
	}

	// b.flush(batchTable, true /* flush */)
	// TODO: 在退出前, 如何优雅得处理剩下的事务?
	close(b.batchQ)
}

func (b *Batcher) appendItemWithFlushOp(batchTable map[uint32]BatchItem, key uint32, source *SourceItem) {
	batch := batchTable[key]

	if batch.NextItemIdx >= b.cfg.MaxBatchSize {
		b.batchQ <- batch
		batch = BatchItem{
			CreatedTime: time.Now(),
			Items:       make([]interface{}, b.cfg.MaxBatchSize),
			NextItemIdx: 0,
		}
	}
	batch.Items[batch.NextItemIdx] = source.item
	batch.NextItemIdx++

	batchTable[key] = batch
}

func (b *Batcher) flush(batchTable map[uint32]BatchItem, flush bool) {
	now := time.Now()
	for key, batch := range batchTable {
		if len(batch.Items) > 0 && (flush || batch.CreatedTime.Add(b.expiredInterval).Before(now)) {
			b.batchQ <- batch
			delete(batchTable, key)
		}
	}
}

func (b *Batcher) sink() {
	var wg sync.WaitGroup
	for i := 0; i < b.cfg.BatcherConcurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for item := range b.batchQ {
				if err := b.doBatch(&item); err != nil {
					log.Error().Err(err).Msgf("batcher-%d does batch job failed", b.id)
				}
				b.processedPerThread[idx] += int64(len(item.Items))
			}
		}(i)
	}
	wg.Wait()
	close(b.done)
}

// Stat 返回批处理器已经处理的批次.
func (b *Batcher) Stat() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var total int64
	for _, x := range b.processedPerThread {
		total += x
	}
	return total
}

// FNV1av32 哈希函数.
// 参考 http://www.isthe.com/chongo/tech/comp/fnv/index.html#FNV-source
func FNV1av32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}
