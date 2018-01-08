package lru

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestLRUCache_Get(t *testing.T) {
	defer glog.Flush()

	created := 0
	destroyed := 0

	pool := NewCache(
		2,
		func(name string) (interface{}, error) {
			created += 1
			glog.Infof("Create %s -> %d", name, created)
			return created, nil
		},
		func(name string, conn interface{}) error {
			destroyed += 1
			glog.Infof("Destroy %s -> %v", name, conn)
			return nil
		},
	)

	value, err := pool.Get("a")
	require.NoError(t, err)
	require.Equal(t, 1, value)
	require.Equal(t, 1, pool.Len())
	require.Equal(t, 1, created)
	require.Equal(t, 0, destroyed)

	value, err = pool.Get("a")
	require.NoError(t, err)
	require.Equal(t, 1, value)
	require.Equal(t, 1, pool.Len())
	require.Equal(t, 1, created)
	require.Equal(t, 0, destroyed)

	value, err = pool.Get("b")
	require.NoError(t, err)
	require.Equal(t, 2, value)
	require.Equal(t, 2, pool.Len())
	require.Equal(t, 2, created)
	require.Equal(t, 0, destroyed)

	value, err = pool.Get("c")
	require.NoError(t, err)
	require.Equal(t, 3, value)
	require.Equal(t, 2, pool.Len())
	require.Equal(t, 3, created)
	require.Equal(t, 1, destroyed)

	value, err = pool.Get("a")
	require.NoError(t, err)
	require.Equal(t, 4, value)
	require.Equal(t, 2, pool.Len())
	require.Equal(t, 4, created)
	require.Equal(t, 2, destroyed)

	err = pool.Purge()
	require.NoError(t, err)
	require.Equal(t, 0, pool.Len())
	require.Equal(t, 4, destroyed)
}

func TestLRUCache_Resize(t *testing.T) {
	created, destroyed := 0, 0

	pool := NewCache(
		1,
		func(name string) (interface{}, error) {
			created++
			return created, nil
		},
		func(name string, e interface{}) error {
			destroyed++
			return nil
		},
	)

	value, err := pool.Get("a")
	require.NoError(t, err)
	require.Equal(t, 1, value)
	require.Equal(t, 1, pool.Len())
	require.Equal(t, 1, created)
	require.Equal(t, 0, destroyed)

	value, err = pool.Get("a")
	require.NoError(t, err)
	require.Equal(t, 1, value)
	require.Equal(t, 1, pool.Len())
	require.Equal(t, 1, created)
	require.Equal(t, 0, destroyed)

	value, err = pool.Get("b")
	require.NoError(t, err)
	require.Equal(t, 2, value)
	require.Equal(t, 1, pool.Len())
	require.Equal(t, 2, created)
	require.Equal(t, 1, destroyed)

	value, err = pool.Get("c")
	require.NoError(t, err)
	require.Equal(t, 3, value)
	require.Equal(t, 1, pool.Len())
	require.Equal(t, 3, created)
	require.Equal(t, 2, destroyed)

	pool.Size = 2

	value, err = pool.Get("d")
	require.NoError(t, err)
	require.Equal(t, 4, value)
	require.Equal(t, 2, pool.Len())
	require.Equal(t, 4, created)
	require.Equal(t, 2, destroyed)

	pool.Size = 1

	value, err = pool.Get("e")
	require.NoError(t, err)
	require.Equal(t, 5, value)
	require.Equal(t, 1, pool.Len())
	require.Equal(t, 5, created)
	require.Equal(t, 4, destroyed)
}

func TestLRUCache_Entries(t *testing.T) {
	defer glog.Flush()

	i := 0
	pool := NewCache(
		2,
		func(name string) (interface{}, error) {
			i++
			return i, nil
		},
		func(name string, value interface{}) error {
			glog.Infof("Destroy %s -> %v", name, value)
			return nil
		},
	)

	pool.Get("a")
	pool.Get("b")
	pool.Get("c")

	entries := pool.Entries()

	require.Len(t, entries, 2)
	require.Equal(t, "c", entries[0].Name)
	require.Equal(t, 3, entries[0].Value)
	require.Equal(t, "b", entries[1].Name)
	require.Equal(t, 2, entries[1].Value)

	pool.Purge()
}

func BenchmarkLRUCache_Get(b *testing.B) {
	defer glog.Flush()

	i := 0

	pool := NewCache(
		100,
		func(name string) (interface{}, error) {
			i++
			return i, nil
		},
		DefaultDestroyFunc,
	)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool.Get(fmt.Sprint(i))
	}
}

func BenchmarkLRUCache_Get_Concurrent(b *testing.B) {
	defer glog.Flush()

	for _, concurrencyLevel := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("level=%d", concurrencyLevel), func(b *testing.B) {
			created := 0
			sem := make(chan int, concurrencyLevel)

			pool := NewCache(
				100,
				func(name string) (interface{}, error) {
					created++
					return created, nil
				},
				DefaultDestroyFunc,
			)

			wg := sync.WaitGroup{}

			for lockId := 0; lockId < concurrencyLevel; lockId++ {
				sem <- lockId
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				wg.Add(1)

				go func() {
					lockId := <-sem
					pool.Get(fmt.Sprint(i))
					sem <- lockId
					wg.Done()
				}()
			}

			wg.Wait()
		})
	}
}
