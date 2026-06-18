## Raft-Go

A simplified implementation of the Raft consensus algorithm written in Go. If you have no idea what Raft is, make sure to read the blog first [How RAFT works](https://medium.com/@syedowais312sf/how-raft-keeps-your-data-safe-when-servers-crash-da3f42c41438) 
### How to run
```bash
go run main.go
```
What happens when you run it
The demo runs through three scenarios automatically.

### Scenario 1 : Cluster starts, leader gets elected
```bash
Node 0 running
Node 1 running
Node 2 running
Node 3 running
Node 4 running
Node 3 timeout, starting election
Node 3 is now leader!
```
All 5 nodes start as followers. Each one gets a random election timer between 150–300ms. Node 3 got lucky — its timer ran out first. It voted for itself, asked everyone else to vote, collected the majority, and became the leader.

### Scenario 2 : Data gets replicated to all followers
```bash
Submitted to Node 3
Node 3 applied command: hello from term 1
Heartbeat success to 4
Commit index updated to 1
Node 2 applied command: hello from term 1
Node 4 applied command: hello from term 1
Node 1 applied command: hello from term 1
Node 0 applied command: hello from term 1
```

Two commands "hello" and "world" are submitted to the leader (Node 3). On the next heartbeat the leader sends them to all followers. Once the majority confirm they have the data, the commit index is updated and every node applies the command.

After this you can see every node has the same log:
```bash
Node 0 log: [{1 hello} {1 world}]
Node 1 log: [{1 hello} {1 world}]
Node 2 log: [{1 hello} {1 world}]
Node 3 log: [{1 hello} {1 world}]
Node 4 log: [{1 hello} {1 world}]
```
The 1 in {1 hello} is the term number — meaning this entry was written during term 1.

### Scenario 3 : Leader crashes, new leader elected, data still safe
Killed leader 3
```bash
Node 1 timeout, starting election
Node 1 is now leader!
```

Node 3 is killed. Followers stop receiving heartbeats and their timers start ticking. Node 1 wins the election and becomes the new leader in term 2.

A new command "after crash" is submitted to the new leader. It replicates it to all followers including the dead Node 3 which is treated as a regular follower now:
```bash
Commit index updated to 2
Node 3 applied command: after crash from term 2
Node 2 applied command: after crash from term 2
Node 1 applied command: after crash from term 2
Node 4 applied command: after crash from term 2
Node 0 applied command: after crash from term 2
```
Final state of every node:
```bash
Node 0 log: [{1 hello} {1 world} {2 after crash}]
Node 1 log: [{1 hello} {1 world} {2 after crash}]
Node 2 log: [{1 hello} {1 world} {2 after crash}]
Node 3 log: [{1 hello} {1 world} {2 after crash}]
Node 4 log: [{1 hello} {1 world} {2 after crash}]
```
Notice "hello" and "world" have term 1 and "after crash" has term 2, reflecting which leader wrote them. Every node has identical logs. No data was lost.

What this implementation covers

- Leader election with randomized timeouts
- Heartbeat mechanism
- Log replication across followers
- Commit index tracking via majority acknowledgement
- Log based vote comparison to prevent stale leaders
- Automatic catch-up for lagging followers via nextIndex stepping

What is simplified vs real Raft
This is a learning implementation. It skips a few production level details like persistent storage, log compaction, and snapshot support. For the full spec check out the official Raft paper — [[link](https://raft.github.io/raft.pdf)]
