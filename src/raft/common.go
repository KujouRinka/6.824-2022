package raft

import "math/rand"

// timeout definition
const (
	ReElectLower = 150
	ReElectUpper = 300

	HeartBeatTimeout = 100
)

func randBetween(lower, upper int) int {
	return rand.Intn(upper-lower) + lower
}
