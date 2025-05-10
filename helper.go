package traft

import (
	"math/rand"
	"time"
)

func RandomElectionTimeout() time.Duration {
	// 随机选举超时时间，范围在 [150ms, 300ms]
	return time.Duration(MinElectionTimeout+rand.Intn(MaxElectionTimeout-MinElectionTimeout)) * time.Millisecond
}
