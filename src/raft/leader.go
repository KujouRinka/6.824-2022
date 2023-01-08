package raft

import (
	"time"
)

type Leader struct{}

func (l *Leader) Run(rf *Raft) {
	DPrintf("%v: LEADER: start", rf.me)
	// make heartbeat timer
	heartbeatInterval := time.Duration(100) * time.Millisecond
	timer := time.NewTimer(heartbeatInterval)
	select {
	case <-timer.C:
		// send heartbeat
		DPrintf("%v: LEADER: send heartbeat", rf.me)
		rf.mu.Lock()
		args := AppendEntriesArgs{
			Term:     rf.term,
			LeaderId: rf.me,
		}
		// send AppendEntries RPCs to all other servers
		replyChan := make(chan AppendEntriesReply, len(rf.peers))
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int) {
				var reply AppendEntriesReply
				rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
				replyChan <- reply
			}(i)
		}
		// make goroutine to check whether it has condition to stay leader
		DPrintf("%v: LEADER: start goroutine to check whether it has condition to stay leader", rf.me)
		successCnt := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			reply := <-replyChan
			if reply.Success && reply.Term == rf.term {
				successCnt++
			}
		}
		if successCnt < len(rf.peers)/2+1 {
			DPrintf("%v: LEADER: not enough success, become FOLLOWER", rf.me)
			rf.state = &Follower{}
		}
		rf.mu.Unlock()
	case vote := <-rf.voteChan:
		if vote.args.Term > rf.term {
			rf.mu.Lock()
			DPrintf("%v: LEADER: become FOLLOWER", rf.me)
			rf.term = vote.args.Term
			rf.voteFor = vote.args.CandidateId
			rf.state = &Follower{}
			rf.mu.Unlock()
			return
		}
	case entry := <-rf.entryChan:
		if entry.args.Term > rf.term {
			rf.mu.Lock()
			DPrintf("%v: LEADER: become FOLLOWER", rf.me)
			rf.term = entry.args.Term
			rf.voteFor = entry.args.LeaderId
			rf.state = &Follower{}
			rf.mu.Unlock()
			return
		}
	}
}
