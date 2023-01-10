package raft

import (
	"6.824/labrpc"
	"time"
)

type Candidate struct{}

func (c *Candidate) Run(rf *Raft) {
	DPrintf("%v %v: CANDIDATE: start", rf.me, rf.curTerm)

	rf.mu.Lock()
	DPrintf("%v: CANDIDATE: add curTerm ok", rf.me)

	// send RequestVote RPC
	replyChan := make(chan RequestVoteReply, len(rf.peers)-1)
	args := RequestVoteArgs{
		Term:        rf.curTerm,
		CandidateId: rf.me,
	}
	DPrintf("%v: CANDIDATE: send RequestVote RPC", rf.me)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(pees *labrpc.ClientEnd) {
			var reply RequestVoteReply
			if pees.Call("Raft.RequestVote", &args, &reply) {
				replyChan <- reply
			}
		}(rf.peers[i])
	}

	grantedCnt := 1
	replyCnt := 1
	minVote := len(rf.peers)/2 + 1
	rf.mu.Unlock()

	timeout := time.Duration(randBetween(ReElectLower, ReElectUpper)) * time.Millisecond
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			DPrintf("%v: CANDIDATE: timer: timeout, start new election", rf.me)
			if replyCnt >= minVote {
				rf.mu.Lock()
				rf.curTerm++
				rf.mu.Unlock()
			}
			return
		case reply := <-replyChan:
			replyCnt++
			if reply.Term > rf.curTerm {
				// become follower
				DPrintf("%v: CANDIDATE: replyChan: become FOLLOWER due to stale curTerm", rf.me)
				rf.mu.Lock()
				rf.curTerm = reply.Term
				rf.votedFor = -1
				rf.state = &Follower{}
				rf.mu.Unlock()
				return
			} else if reply.Term == rf.curTerm && reply.VoteGranted {
				DPrintf("%v: CANDIDATE: replyChan: receive valid granted vote with term: %v", rf.me, reply.Term)
				grantedCnt++
				if grantedCnt >= minVote {
					DPrintf("%v: CANDIDATE replyChan: become LEADER", rf.me)
					rf.mu.Lock()
					rf.state = &Leader{}
					rf.mu.Unlock()
					return
				}
			}
		case vote := <-rf.voteChan:
			rf.mu.Lock()
			if vote.args.Term > rf.curTerm {
				DPrintf("%v: CANDIDATE: voteChan: become FOLLOWER due to stale curTerm", rf.me)
				rf.curTerm = vote.args.Term
				rf.votedFor = vote.args.CandidateId
				rf.state = &Follower{}
				rf.mu.Unlock()
				vote.reply.VoteGranted = true
				vote.reply.Term = vote.args.Term
				vote.notify <- struct{}{}
				return
			} else if vote.args.Term < rf.curTerm {
				DPrintf("%v: CANDIDATE: voteChan: stale vote request", rf.me)
				vote.reply.VoteGranted = false
				vote.reply.Term = rf.curTerm
			} else {
				DPrintf("%v: CANDIDATE: voteChan: request vote from: %v, reject", rf.me, vote.args.CandidateId)
				vote.reply.VoteGranted = false
				vote.reply.Term = rf.curTerm
			}
			rf.mu.Unlock()
			vote.notify <- struct{}{}
		case entry := <-rf.entryChan:
			rf.mu.Lock()
			// a leader send AppendEntries, if args.term >= curTerm
			// acknowledge the leader and become follower
			// or reject the request then stay as candidate
			if entry.args.Term >= rf.curTerm {
				DPrintf("%v: CANDIDATE: entryChan: become FOLLOWER due to stale curTerm", rf.me)
				rf.curTerm = entry.args.Term
				rf.votedFor = entry.args.LeaderId
				rf.state = &Follower{}
				rf.mu.Unlock()
				entry.reply.Term = entry.args.Term
				entry.reply.Success = true
				entry.notify <- struct{}{}
				return
			} else {
				DPrintf("%v: CANDIDATE: entryChan: stale AppendEntries request", rf.me)
				entry.reply.Term = rf.curTerm
				entry.reply.Success = false
			}
		}
	}
}
