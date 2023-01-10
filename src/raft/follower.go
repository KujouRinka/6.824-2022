package raft

import (
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
	DPrintf("%v %v: FOLLOWER: start", rf.me, rf.curTerm)
	// make a random timeout between 150 and 300ms
	timeout := time.Duration(randBetween(ReElectLower, ReElectUpper)) * time.Millisecond
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			// timeout, become candidate
			DPrintf("%v: FOLLOWER: timer: timeout, become candidate", rf.me)
			rf.mu.Lock()
			rf.state = &Candidate{}
			rf.curTerm++
			rf.votedFor = rf.me
			rf.mu.Unlock()
			return
		case vote := <-rf.voteChan:
			// receive RequestVote RPC
			vote.reply.Term = vote.args.Term
			vote.reply.VoteGranted = false

			DPrintf("%v: FOLLOWER: voteChan: receive RequestVote from %v", rf.me, vote.args.CandidateId)
			rf.mu.Lock()
			if vote.args.Term < rf.curTerm {
				DPrintf("%v: FOLLOWER: voteChan: reject RequestVote from %v: Stale Term, current is %v, remote is %v",
					rf.me,
					vote.args.CandidateId,
					rf.curTerm,
					vote.args.Term)
				rf.mu.Unlock()
				vote.notify <- struct{}{}
				continue
			}

			// grant vote, update curTerm
			DPrintf("%v: FOLLOWER: voteChan: update curTerm from %v to %v", rf.me, rf.curTerm, vote.args.Term)
			if (vote.args.Term == rf.curTerm && rf.votedFor == -1) ||
				//rf.votedFor == vote.args.CandidateId ||
				vote.args.Term > rf.curTerm {
				rf.curTerm = vote.args.Term
				rf.votedFor = vote.args.CandidateId
				vote.reply.Term = rf.curTerm
				vote.reply.VoteGranted = true
			}
			vote.notify <- struct{}{}
			rf.mu.Unlock()

			// reset timer
			DPrintf("%v: FOLLOWER: voteChan: reset timer", rf.me)
			timeout = time.Duration(randBetween(ReElectLower, ReElectUpper)) * time.Millisecond
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(timeout)
		case entry := <-rf.entryChan:
			// receive AppendEntries RPC
			// now we just handle heartbeat in 2A
			DPrintf("%v: FOLLOWER: entryChan: receive AppendEntries from %v", rf.me, entry.args.LeaderId)
			rf.mu.Lock()
			if entry.args.Term < rf.curTerm {
				// stale curTerm, do nothing
				entry.reply.Term = rf.curTerm
				entry.reply.Success = false
			} else if entry.args.Term > rf.curTerm {
				// larger curTerm, may this server is out of date
				DPrintf("%v: FOLLOWER: entryChan: update curTerm from %v to %v", rf.me, rf.curTerm, entry.args.Term)
				rf.curTerm = entry.args.Term
				rf.votedFor = -1
				entry.reply.Term = rf.curTerm
				entry.reply.Success = true
				timeout = time.Duration(randBetween(ReElectLower, ReElectUpper)) * time.Millisecond
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(timeout)
			} else {
				// same curTerm, reset timer
				// TODO: add AppendEntries for further implementation
				DPrintf("%v: FOLLOWER: entryChan: AppendEntries from %v, reset timer", rf.me, entry.args.LeaderId)
				entry.reply.Term = rf.curTerm
				entry.reply.Success = true
				timeout = time.Duration(randBetween(ReElectLower, ReElectUpper)) * time.Millisecond
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(timeout)
			}
			rf.mu.Unlock()
			entry.notify <- struct{}{}
			DPrintf("%v: FOLLOWER: entryChan: notify ok", rf.me)
		}
	}
}
