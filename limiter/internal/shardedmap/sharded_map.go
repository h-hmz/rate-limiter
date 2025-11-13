package shardedmap

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

type Shard[T any] struct {
	sync.RWMutex
	m map[string]T
}

type ShardedMap[T any] []*Shard[T]

func NewShardedMap[T any](nShards int) ShardedMap[T] {
	shards := make([]*Shard[T], nShards)

	for i := range nShards {
		shards[i] = &Shard[T]{m: make(map[string]T)}
	}

	return shards
}

func (sm ShardedMap[T]) getShard(key string) *Shard[T] {
	h := xxhash.Sum64String(key)
	idx := int(h % uint64(len(sm)))
	return sm[idx]
}

func (sm ShardedMap[T]) Get(key string) (T, bool) {
	shard := sm.getShard(key)
	shard.RLock()
	defer shard.RUnlock()

	val, ok := shard.m[key]
	return val, ok
}

func (sm ShardedMap[T]) Set(key string, value T) {
	shard := sm.getShard(key)
	shard.Lock()
	defer shard.Unlock()

	shard.m[key] = value
}

func (sm ShardedMap[T]) WithShard(key string, init func() T, fn func(T) (T, bool)) (T, bool) {
	shard := sm.getShard(key)
	shard.Lock()
	defer shard.Unlock()

	val, ok := shard.m[key]
	if !ok {
		val = init()
	}
	shard.m[key], ok = fn(val)
	return shard.m[key], ok
}
