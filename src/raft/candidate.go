package raft

import (
	"math/rand"
	"time"
)

type Candidate struct{}

func (c *Candidate) Run(rf *Raft) {
	DPrintf("%v: CANDIDATE: start", rf.me)

	rf.mu.Lock()
	rf.term++
	rf.voteFor = rf.me
	rf.mu.Unlock()
	DPrintf("%v: CANDIDATE: add term ok", rf.me)
	grantChan := make(chan bool)
	go func() {
		// increment current term and send RequestVote RPCs to all other servers
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("%v: CANDIDATE: send RequestVote to all other servers", rf.me)
		args := RequestVoteArgs{
			Term:        rf.term,
			CandidateId: rf.me,
		}
		replyChan := make(chan RequestVoteReply)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int) {
				var reply RequestVoteReply
				rf.peers[server].Call("Raft.RequestVote", &args, &reply)
				replyChan <- reply
			}(i)
		}

		// wait for votes
		curVote := 1
		curGranted := 1
		minVote := len(rf.peers)/2 + 1
		for curVote < len(rf.peers) && curGranted < minVote {
			reply := <-replyChan
			curVote++
			if reply.VoteGranted && reply.Term == rf.term {
				curGranted++
			}
		}

		// make goroutine to consume all remaining replies
		go func() {
			for curVote < len(rf.peers) {
				<-replyChan
				curVote++
			}
		}()

		if curGranted >= minVote {
			grantChan <- true
		} else {
			grantChan <- false
		}
		DPrintf("%v: CANDIDATE: SENDING FUNCTION EXIT", rf.me)
	}()

	// make a random timeout between 150 and 300ms
	timeout := time.Duration(rand.Intn(150)+150) * time.Millisecond
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			// timeout, re-elect
			DPrintf("%v: CANDIDATE: timeout, re-elect", rf.me)
			return
		case vote := <-rf.voteChan:
			// receive RequestVote RPC, reject
			vote.reply.Term = rf.term
			vote.reply.VoteGranted = false
			DPrintf("%v: CANDIDATE: receive RequestVote from %v, reject", rf.me, vote.args.CandidateId)
			vote.notify <- struct{}{}
		case entry := <-rf.entryChan:
			// receive AppendEntries RPC, if term is larger, become follower
			DPrintf("%v: CANDIDATE: receive AppendEntries from %v", rf.me, entry.args.LeaderId)
			if entry.args.Term > rf.term {
				DPrintf("%v: CANDIDATE: become FOLLOWER", rf.me)
				rf.mu.Lock()
				rf.term = entry.args.Term
				rf.voteFor = entry.args.Term
				rf.state = &Follower{}
				rf.mu.Unlock()
				return
			}
			entry.notify <- struct{}{}
		case granted := <-grantChan:
			// receive enough votes, decide whether to become leader
			if granted {
				DPrintf("%v: CANDIDATE: requiring lock", rf.me)
				rf.mu.Lock()
				rf.state = &Leader{}
				rf.mu.Unlock()
				DPrintf("%v: CANDIDATE: become LEADER", rf.me)
				return
			} else {
				DPrintf("%v: CANDIDATE: not enough votes, re-elect", rf.me)
				return
			}
		}
	}
}
