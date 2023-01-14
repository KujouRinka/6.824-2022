package raft

import (
	"math/rand"
	"time"
)

// timeout definition
const (
	ReElectLower = 150
	ReElectUpper = 300

	HeartBeatTimeout = 100
)

func randBetween(lower, upper int) int {
	return rand.Intn(upper-lower) + lower
}

func electionTimeout() time.Duration {
	return time.Duration(randBetween(ReElectLower, ReElectUpper)) * time.Millisecond
}

func heartbeatTimeout() time.Duration {
	return time.Duration(HeartBeatTimeout) * time.Millisecond
}

func resetTimer(timer *time.Timer, timeout time.Duration) {
	if !timer.Stop() {
		<-timer.C
	}
	timer.Reset(timeout)
}
