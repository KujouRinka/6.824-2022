package raft

import (
	"math/rand"
	"time"
)

// state definition
const (
	Follower = iota
	Candidate
	Leader
)

type RState int

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
	return HeartBeatTimeout * time.Millisecond
}

func resetTimer(timer *time.Timer, timeout time.Duration) {
	timer.Reset(timeout)
}
