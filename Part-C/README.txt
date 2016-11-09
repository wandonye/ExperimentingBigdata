Note: PartCQuestion1.sh and PartCQuestion2.sh run in local mode. To run in cluster, add remote as an additional parameter:
~/software/apache-storm-1.0.2/bin/storm jar ~/assignment_2/part_c/storm-starter-1.0.2.jar org.apache.storm.starter.PartC1 remote

~/software/apache-storm-1.0.2/bin/storm jar ~/assignment_2/part_c/storm-starter-1.0.2.jar org.apache.storm.starter.PartC2 remote

Q1:
Topology file: PartC1.java
We used one spout and one bolt for this task. The spout we used is TwitterSampleSpout, which in charge of emitting twitter with a list of keywords. We made small modification to the TwitterSampleSpout in storm starter project, so that it only emit twitter text. We implemented a bolt, WriteBolt. This bolt takes in the twitter text from TwitterSampleSpout.
The result file "tweets.txt" will be stored in the same folder as the runner scripts, i.e. ~/grader/


Q2:
Topology file: PartC2.java
We have 3 spouts and 4 bolts, see the PartCQ2topology.jpeg for a diagram.
We implemented the following 3 spouts:
1.TwitterStreamSpout: connects to Twitter API and emits English tweets. This spout called "twitter" in our topology.
2.RandomHashTagsSpout: It contains a list of predefined hashtags. Every 30 seconds, it will randomly sample and propagate a subset of hashtags. This spout called "hashtags" in the topology.
3.FriendCountSpout: Every 30 seconds, it will emit a random number which is <30000. This spout called "friendscount" in the topology.

The streams from the above three spout were supplied to a bolt called SelectorBolt. In this bolt, we filter the tweet with respect to the given condition (have hashtags from hashtags spout and have friendsCount<the friendsCount from the friendscount spout). This spout will emit tuples consist of the id and text of each tweet.

Our next bolt in the pipeline is WordSplitterBolt, which takes the [id,text] tuple, split the text into words and emit [id, text, word]. Here we keep text together with word to make sure in the output to file stage, the word count files and tweet files are perfectly aligned. This will be explained later.

The next bolt, IgnoreWordsBolt, takes in the tuple [id, text, word] from WordSplitterBolt and check if the word belongs to the stop word set. If so, the tuple will be filtered out. The rest tuples will be emitted without change.

The tuples [id, text, word] from IgnoreWordsBolt are then piped into the final bolt, RollingCountWriteBolt. This bolt does three things:
1. it keeps a set of all tweets from the previous bolt. Duplicated tweets were removed using tweet id. At every 30 seconds, it will save the tweets to a file called "tweets"+current_timestamp+".txt" and clear the list.
2. it keeps a map of all word and their counts. At every 30 seconds, it will save the top 50% words to a file called "top"+current_timestamp+".txt" and clear the list.
Note that current_timestamp are the same for the same time window.

The main difficult to get the required number of tweets is that, most tweets doesn't have hashtags.
