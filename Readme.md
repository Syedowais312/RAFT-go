#Raft algo

here we first create nodes, and then run the main funscion, in which every node intialized it self, and the comments are self explanatory for better understanding.

once we intialize the node we run the election fucntion, to make some random node a leader,
This finction runs for infinite, until some node lucky acrosses the bounde time(150 miliseconds)
and then for this node node we run the startelection, where the node first vote itself, and tell other notes to vote it.
as describe in the if all the conditions are meet for the other node( for example the data that the leader node having is more than the current node , the current node will vote the leader)

once the node is the leader, its job is to sent the heartbeat saying "the leader is still alive" and sync the data of leader to the follower nodes