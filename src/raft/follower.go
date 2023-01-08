package raft

import (
	"math/rand"
	"time"
)

// type RState int
//
// const (
// 	Follower RState = iota
// 	Candidate
// 	Leader
// )

type Follower struct{}

func (f *Follower) Run(rf *Raft) {
	DPrintf("%v: FOLLOWER: start", rf.me)
	// make a random timeout between 150 and 300ms
	timeout := time.Duration(rand.Intn(150)+150) * time.Millisecond
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			// timeout, become candidate
			DPrintf("%v: FOLLOWER: timeout, become candidate", rf.me)
			rf.mu.Lock()
			rf.state = &Candidate{}
			rf.mu.Unlock()
			return
		case vote := <-rf.voteChan:
			// receive RequestVote RPC
			vote.reply.Term = rf.term
			vote.reply.VoteGranted = false

			DPrintf("%v: FOLLOWER: receive RequestVote from %v", rf.me, vote.args.CandidateId)
			rf.mu.Lock()
			if vote.args.Term <= rf.term {
				DPrintf("%v: FOLLOWER: reject RequestVote from %v: Stale Term, current is %v, remote is %v",
					rf.me,
					vote.args.CandidateId,
					rf.term,
					vote.args.Term)
				rf.mu.Unlock()
				vote.notify <- struct{}{}
				continue
			}

			// update term
			DPrintf("%v: FOLLOWER: update term from %v to %v", rf.me, rf.term, vote.args.Term)
			rf.term = vote.args.Term
			rf.voteFor = vote.args.CandidateId
			vote.reply.Term = rf.term
			vote.reply.VoteGranted = true
			vote.notify <- struct{}{}
			rf.mu.Unlock()

			// reset timer
			DPrintf("%v: FOLLOWER: reset timer", rf.me)
			timeout = time.Duration(rand.Intn(150)+150) * time.Millisecond
			timer.Reset(timeout)
		case entry := <-rf.entryChan:
			// receive AppendEntries RPC
			// now we just handle heartbeat in 2A
			DPrintf("%v: FOLLOWER: receive AppendEntries from %v", rf.me, entry.args.LeaderId)
			rf.mu.Lock()
			if entry.args.Term < rf.term {
				// stale term, do nothing
			} else if entry.args.Term > rf.term {
				// larger term, may this server is out of date
				DPrintf("%v: FOLLOWER: update term from %v to %v", rf.me, rf.term, entry.args.Term)
				rf.term = entry.args.Term
				rf.voteFor = entry.args.LeaderId
				timeout = time.Duration(rand.Intn(150)+150) * time.Millisecond
				timer.Reset(timeout)
			} else {
				// same term, reset timer
				// TODO: add AppendEntries for further implementation
				DPrintf("%v: FOLLOWER: AppendEntries from %v, reset timer", rf.me, entry.args.LeaderId)
				timeout = time.Duration(rand.Intn(150)+150) * time.Millisecond
				timer.Reset(timeout)
			}
			rf.mu.Unlock()
			entry.notify <- struct{}{}
			DPrintf("%v: FOLLOWER: notify ok", rf.me)
		}
	}
}
