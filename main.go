package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Leader State = iota
	Follower
	Candidate
)

type LogEntry struct {
	Term    int
	Command interface{}
}
type Node struct {
	peers       []*Node
	ID          int
	currstate   State
	term        int
	voted       int
	timer       time.Time
	alive       bool
	log         []LogEntry
	commitIndex int
	nextIndex   []int
	matchIndex  []int
	mu          sync.Mutex
}
type RequestVoteArgs struct {
	term         int
	candidateID  int
	lastLogIndex int
	lastLogTerm  int
}
type RequestVoteReply struct {
	candidateID int
	term        int
	vote        bool
}

type AppendEntries struct {
	term         int
	leaderID     int
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry
	leaderCommit int
}

type AppendEntriesReply struct {
	term    int
	success bool
}

func main() {
	node := make([]*Node, 5)

	// Initialize nodes
	for i := 0; i < 5; i++ {
		node[i] = &Node{
			ID:        i,
			currstate: Follower,
			term:      0,
			voted:     -1,
			alive:     true,
		}
		go node[i].applyLoop()

	}
	// Set peers for each node
	for i, n := range node {
		for j, m := range node {
			if i != j {
				n.peers = append(n.peers, m)
			}
		}
	}
	// Start election timers for each node
	for i, n := range node {
		go n.runElectionTimer()
		fmt.Println("Node", i, "runing")
	}
	time.Sleep(500 * time.Millisecond)
	for _, n := range node {
		if n.currstate == Leader {
			// submit some commands to the leader as entry
			n.Submit("hello")
			n.Submit("world")
			fmt.Println("Submitted to Node", n.ID)
		}
	}
	time.Sleep(500 * time.Millisecond)
	for _, n := range node {
		fmt.Println("Node", n.ID, "log:", n.log)
	}

	// kill the leader mid-way
	for _, n := range node {
		if n.currstate == Leader {
			n.alive = false
			fmt.Println("Killed leader", n.ID)
		}
	}
	time.Sleep(1 * time.Second)
	// submit to new leader
	for _, n := range node {
		if n.currstate == Leader {
			n.Submit("after crash")
		}
	}
	time.Sleep(500 * time.Millisecond)
	for _, n := range node {
		fmt.Println("Node", n.ID, "log:", n.log)
	}

}

func (n *Node) applyLoop() {
	lastApplied := -1
	for {
		time.Sleep(10 * time.Millisecond)
		n.mu.Lock()
		if lastApplied < n.commitIndex && lastApplied+1 < len(n.log) {
			lastApplied++
			entry := n.log[lastApplied]
			fmt.Printf("Node %d applied command: %v from term %d\n", n.ID, entry.Command, entry.Term)
		}
		n.mu.Unlock()
	}
}
func (n *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()
	lastLogIndex := len(n.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = n.log[lastLogIndex].Term
	}

	if args.lastLogTerm < lastLogTerm {
		reply.vote = false
		return
	}
	if args.lastLogTerm == lastLogTerm && args.lastLogIndex < lastLogIndex {
		reply.vote = false
		return
	}
	if args.term < n.term {
		reply.vote = false
		return
	}

	if args.term > n.term {
		n.term = args.term
		n.voted = -1
		n.currstate = Follower
	}
	if n.voted != -1 {
		reply.vote = false
		return
	}
	n.voted = args.candidateID
	reply.candidateID = n.ID
	n.term = args.term
	reply.vote = true

	n.timer = time.Now()
}
func (n *Node) runElectionTimer() {

	time.Sleep(time.Duration(rand.Intn(150)) * time.Millisecond)
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	n.mu.Lock()
	n.timer = time.Now()
	n.mu.Unlock()
	for {
		time.Sleep(10 * time.Millisecond)
		n.mu.Lock()
		if !n.alive {
			fmt.Println("Node", n.ID, "is dead")
			n.mu.Unlock()
			return
		} else if n.currstate == Leader {
			fmt.Println("Node", n.ID, "is leader, resetting timer")
			n.mu.Unlock()
			return
		} else if time.Since(n.timer) > timeout {
			fmt.Println("Node", n.ID, "timeout, starting election")
			// start election for the node that lucks out and becomes candidate
			go n.startElection()
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
	}
}
func (n *Node) startElection() {
	n.mu.Lock()
	if n.currstate == Leader {
		n.mu.Unlock()
		return // aborted before we even started
	}
	// make this node a candidate and start election(if follower or candidate)
	n.currstate = Candidate
	// increment term and vote for self
	n.term++
	currentTerm := n.term
	n.voted = n.ID
	n.mu.Unlock()
	voteCh := make(chan bool, len(n.peers))

	n.mu.Lock()
	lastLogIndex := len(n.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = n.log[lastLogIndex].Term
	}
	n.mu.Unlock()
	for _, p := range n.peers {
		go func(peer *Node) {
			args := RequestVoteArgs{
				term:         currentTerm,
				candidateID:  n.ID,
				lastLogIndex: lastLogIndex,
				lastLogTerm:  lastLogTerm,
			}

			reply := RequestVoteReply{}
			peer.RequestVote(args, &reply)
			voteCh <- reply.vote
		}(p)
	}

	votes := 1
	for i := 0; i < len(n.peers); i++ {
		if <-voteCh {
			votes++
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.term != currentTerm {
		n.currstate = Follower
		n.voted = -1
		go n.runElectionTimer()
		return
	}
	// if majority votes received, become leader
	if votes > len(n.peers)/2 {
		if n.currstate != Candidate {
			return
		}
		n.currstate = Leader
		n.nextIndex = make([]int, len(n.peers))
		n.matchIndex = make([]int, len(n.peers))
		for i := range n.nextIndex {
			n.nextIndex[i] = len(n.log)
			n.matchIndex[i] = -1
		}
		go n.sendHeartbeat()
		fmt.Println("Node", n.ID, "is now leader!")
	} else {
		n.currstate = Follower
		n.voted = -1
		go n.runElectionTimer()

	}

}

// func (n *Node) RecieveHeartbeat(leaderID int, term int) {
// 	n.mu.Lock()
// 	if term < n.term {
// 		n.mu.Unlock()
// 		return
// 	}

// 	if term >= n.term {
// 		n.term = term
// 		n.voted = -1
// 		n.currstate = Follower
// 		n.timer = time.Now()
// 	}
// 	fmt.Println("Node", n.ID, "received heartbeat from leader", leaderID, "with term", term)
// 	n.timer = time.Now()
// 	n.mu.Unlock()

// }

func (n *Node) sendHeartbeat() {
	for {
		time.Sleep(50 * time.Millisecond)
		n.mu.Lock()
		if n.currstate != Leader || !n.alive {
			n.mu.Unlock()
			return
		}
		for i, p := range n.peers {
			go func(peer *Node, peerIndex int) {
				n.mu.Lock()
				prevLogIndex := n.nextIndex[peerIndex] - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 {
					prevLogTerm = n.log[prevLogIndex].Term
				}
				entries := n.log[n.nextIndex[peerIndex]:]
				args := AppendEntries{
					term:         n.term,
					leaderID:     n.ID,
					prevLogIndex: prevLogIndex,
					prevLogTerm:  prevLogTerm,
					entries:      entries,
					leaderCommit: n.commitIndex,
				}
				n.mu.Unlock()
				AppendEntriesReply := AppendEntriesReply{}
				peer.AppendEntries(args, &AppendEntriesReply)
				n.mu.Lock()
				if AppendEntriesReply.success {
					// if heartbeat is successful, update nextIndex for that peer
					n.nextIndex[peerIndex] = 1 + prevLogIndex + len(args.entries)

					n.matchIndex[peerIndex] = n.nextIndex[peerIndex] - 1

					for idx := len(n.log) - 1; idx > n.commitIndex; idx-- {
						count := 1
						for _, match := range n.matchIndex {
							if match >= idx {
								count++
							}

						}
						if count > len(n.peers)/2 {
							n.commitIndex = idx
							fmt.Println("Commit index updated to ", n.commitIndex)
							break
						}
					}
					fmt.Println("Heartbeat success to ", peer.ID)
				} else {
					// if heartbeat fails, decrement nextIndex for that peer and try again in next heartbeat
					if n.nextIndex[peerIndex] > 0 {
						n.nextIndex[peerIndex]--
						fmt.Println("Heartbeat failed to ", peer.ID, "decreasing nextIndex to ", n.nextIndex[peerIndex])

					}
				}
				n.mu.Unlock()
			}(p, i)
			fmt.Println("Heartbeat send to ", p.ID)
		}
		n.mu.Unlock()
	}
}

func (n *Node) Submit(command interface{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.currstate != Leader {
		fmt.Println("Node", n.ID, "is not leader, cannot submit command")
		return
	}
	n.log = append(n.log, LogEntry{
		Term:    n.term,
		Command: command,
	})

}

func (n *Node) AppendEntries(args AppendEntries, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.term < n.term {
		reply.term = n.term
		reply.success = false
		return
	}

	n.timer = time.Now()
	n.term = args.term
	n.currstate = Follower

	// if len(n.log) <= args.prevLogIndex {
	// 	fmt.Println("Leader has more logs")
	// 	reply.success = false
	// 	return
	// }
	if args.prevLogIndex >= 0 {
		if len(n.log) <= args.prevLogIndex {
			reply.success = false
			return
		}
		if n.log[args.prevLogIndex].Term != args.prevLogTerm {
			fmt.Println("Log term mismatch")
			reply.success = false
			return
		}
	}

	n.log = append(n.log[:args.prevLogIndex+1], args.entries...)

	if args.leaderCommit > n.commitIndex {
		n.commitIndex = args.leaderCommit
	}
	reply.success = true
}
