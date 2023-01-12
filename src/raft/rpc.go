package raft

import "6.824/labrpc"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's curTerm
	CandidateId int // candidate requesting vote
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type voteParam struct {
	args   *RequestVoteArgs
	reply  *RequestVoteReply
	notify chan struct{}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v %v: received RequestVote from: %v %v", rf.me, rf.curTerm, args.CandidateId, args.Term)

	if args.Term < rf.curTerm ||
		(args.Term == reply.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		DPrintf("%v %v: reject RequestVote from: %v %v: stale term", rf.me, rf.curTerm, args.CandidateId, args.Term)
		reply.Term = rf.curTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.curTerm {
		DPrintf("%v %v: update term to %v due to stale term", rf.me, rf.curTerm, args.Term)
		rf.curTerm = args.Term
		rf.state = Follower
	}
	rf.votedFor = args.CandidateId
	resetTimer(rf.electionTimer, electionTimeout())

	reply.Term = rf.curTerm
	reply.VoteGranted = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server *labrpc.ClientEnd,
	args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := server.Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int // leader's curTerm
	LeaderId int // so follower can redirect clients
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

type appendEntryParam struct {
	args   *AppendEntriesArgs
	reply  *AppendEntriesReply
	notify chan struct{}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v %v: received AppendEntries from: %v %v", rf.me, rf.curTerm, args.LeaderId, args.Term)

	if args.Term < rf.curTerm {
		DPrintf("%v %v: reject AppendEntries from: %v %v", rf.me, rf.curTerm, args.LeaderId, args.Term)
		reply.Term = rf.curTerm
		reply.Success = false
		return
	}
	if args.Term > rf.curTerm {
		DPrintf("%v %v: update term to %v due to stale term", rf.me, rf.curTerm, args.Term)
		rf.curTerm = args.Term
		rf.state = Follower
	}
	resetTimer(rf.electionTimer, electionTimeout())

	reply.Term = rf.curTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server *labrpc.ClientEnd,
	args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := server.Call("Raft.AppendEntries", args, reply)
	return ok
}
