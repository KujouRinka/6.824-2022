package raft

import (
	"6.824/labrpc"
	"time"
)

type Leader struct{}

func (l *Leader) Run(rf *Raft) {
	DPrintf("%v %v: LEADER: start", rf.me, rf.curTerm)

	// send heartbeat
	DPrintf("%v: LEADER: send heartbeat", rf.me)
	rf.mu.Lock()
	replyChan := make(chan AppendEntriesReply, len(rf.peers))
	args := AppendEntriesArgs{
		Term:     rf.curTerm,
		LeaderId: rf.me,
	}
	// send AppendEntries RPCs to all other servers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer *labrpc.ClientEnd) {
			var reply AppendEntriesReply
			if peer.Call("Raft.AppendEntries", &args, &reply) {
				replyChan <- reply
			}
		}(rf.peers[i])
	}
	rf.mu.Unlock()

	// make heartbeat timer
	timer := time.NewTimer(heartbeatTimeout())
	for {
		select {
		case <-timer.C:
			return
		case reply := <-replyChan:
			rf.mu.Lock()
			if reply.Term > rf.curTerm {
				DPrintf("%v: LEADER: replyChan: become FOLLOWER due to stale curTerm", rf.me)
				rf.curTerm = reply.Term
				rf.votedFor = -1
				rf.state = &Follower{}
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		case vote := <-rf.voteChan:
			rf.mu.Lock()
			if vote.args.Term > rf.curTerm {
				DPrintf("%v: LEADER: voteChan: become FOLLOWER", rf.me)
				rf.curTerm = vote.args.Term
				rf.votedFor = vote.args.CandidateId
				rf.state = &Follower{}
				vote.reply.VoteGranted = true
				vote.reply.Term = rf.curTerm
				rf.mu.Unlock()
				vote.notify <- struct{}{}
				return
			} else if vote.args.Term <= rf.curTerm {
				DPrintf("%v: LEADER: voteChan: vote denied", rf.me)
				vote.reply.VoteGranted = false
				vote.reply.Term = rf.curTerm
			}
			rf.mu.Unlock()
			vote.notify <- struct{}{}
		case entry := <-rf.entryChan:
			rf.mu.Lock()
			if entry.args.Term > rf.curTerm {
				DPrintf("%v: LEADER: entryChan: become FOLLOWER", rf.me)
				rf.curTerm = entry.args.Term
				rf.votedFor = entry.args.LeaderId
				rf.state = &Follower{}
				entry.reply.Success = true
				entry.reply.Term = rf.curTerm
				rf.mu.Unlock()
				entry.notify <- struct{}{}
				return
			} else if entry.args.Term <= rf.curTerm {
				DPrintf("%v: LEADER: entryChan: entry denied", rf.me)
				entry.reply.Success = false
				entry.reply.Term = rf.curTerm
			}
			rf.mu.Unlock()
			entry.notify <- struct{}{}
		}
	}
}
