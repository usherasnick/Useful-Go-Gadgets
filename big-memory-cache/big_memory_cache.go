package bigmemcache

import (
	"time"

	"github.com/allegro/bigcache"
)

const (
	__DefaultEvictionTime = 100 * 365 * 24 * time.Hour
	__DefaultShardsFactor = 100
	__DefaultMaxShards    = 128
	__OneMB               = 1024 * 1024
)

// BigMemCacheCfg BigMemCache配置
type BigMemCacheCfg struct {
	MaxNumOfCacheItem  uint64 // 最多可缓存的对象数量
	MaxSizeOfCacheItem uint64 // 对象大小, unit is byte
}

func (cfg *BigMemCacheCfg) defaultBigCacheCfg() bigcache.Config {
	bcCfg := bigcache.DefaultConfig(__DefaultEvictionTime)
	bcCfg.Verbose = false

	shardsUpLimit := uint(cfg.MaxNumOfCacheItem/__DefaultShardsFactor) + 1
	bcCfg.Shards = int(findNearestPowerOf2Num(shardsUpLimit))
	if bcCfg.Shards > __DefaultMaxShards {
		bcCfg.Shards = __DefaultMaxShards
	}

	// init 10 entries for each shard.
	bcCfg.MaxEntriesInWindow = 10 * bcCfg.Shards
	bcCfg.MaxEntrySize = int(cfg.MaxSizeOfCacheItem)

	bcCfg.HardMaxCacheSize = int((cfg.MaxNumOfCacheItem*cfg.MaxSizeOfCacheItem)/__OneMB) + 1
	return bcCfg
}

// Feature 范指代AI领域的特征对象.
type Feature struct {
	Version     int32
	UUID        string
	Meta        []byte
	Blob        []byte
	CreatedTime int64
}

// BigMemCache stores serialized items (feature as example) in memory.
// Item is serialized as []byte to avoid excessive GC stress and extra memory footprint.
type BigMemCache struct {
	cache *bigcache.BigCache
}

// NewBigMemCache 返回BigMemCache实例.
func NewBigMemCache(cfg *BigMemCacheCfg) (*BigMemCache, error) {
	cache, err := bigcache.NewBigCache(cfg.defaultBigCacheCfg())
	if err != nil {
		return nil, err
	}
	return &BigMemCache{
		cache: cache,
	}, nil
}

// Add 将特征对象添加进BigMemCache.
func (bmc *BigMemCache) Add(fe *Feature) error {
	encoded, err := bmc.encode(fe)
	if err != nil {
		return err
	}
	return bmc.cache.Set(fe.UUID, encoded)
}

// Del 将特征对象从BigMemCache删除.
func (bmc *BigMemCache) Del(uuid string) error {
	// mark-deletion in bigcache
	return bmc.cache.Delete(uuid)
}

// Get 从BigMemCache中获取特征对象.
func (bmc *BigMemCache) Get(uuid string) *Feature {
	v, err := bmc.cache.Get(uuid)
	if err != nil || v == nil {
		return nil
	}
	fe, err := bmc.decode(v)
	if err != nil {
		return nil
	}
	return fe
}

// Size 返回BigMemCache当前缓存的对象数量.
func (bmc *BigMemCache) Size() int {
	return bmc.cache.Len()
}

// Reset 真正意义上去清理缓存.
func (bmc *BigMemCache) Reset() error {
	return bmc.cache.Reset()
}
