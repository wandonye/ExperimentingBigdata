/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.starter.bolt.IgnoreWordsBolt;
import org.apache.storm.starter.bolt.RollingCountWriteBolt;
import org.apache.storm.starter.bolt.SelectorBolt;
import org.apache.storm.starter.bolt.WordSplitterBolt;
import org.apache.storm.starter.spout.TwitterStreamSpout;
import org.apache.storm.starter.spout.FriendCountSpout;
import org.apache.storm.starter.spout.RandomHashTagsSpout;
import org.apache.storm.starter.util.StormRunner;

public class PartC2 {
	private final TopologyBuilder builder;
    private final String topologyName = "PartC2";
    private final Config topologyConfig;
    private static final int DEFAULT_RUNTIME_IN_SECONDS = 6000;
    private final int runtimeInSeconds;
    
    public PartC2() throws InterruptedException {
        builder = new TopologyBuilder();
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
        wireTopology();
    }
    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        conf.setDebug(true);
        return conf;
    }
    
    private void wireTopology() throws InterruptedException {
    	String consumerKey = "phhCqmJBYarJE2kX74oVT6L1m"; 
        String consumerSecret = "n9WjhY81IFi3zy0qL69YxDnwkAYuamaaMINcbt7RBbOGUC1ObV"; 
        String accessToken = "56472627-tepJv0DiQOniiUxVcDbh7gDfr7TxWFjDkeoJ4L6ht"; 
        String accessTokenSecret = "3C3MIXeJCdVThTIgooiwqGF6iGgD4lUsKnx0KMVLWJGB7";
        String[] keyWords = {};

        builder.setSpout("twitter", new TwitterStreamSpout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, keyWords),4);
        builder.setSpout("hashtags", new RandomHashTagsSpout());
        builder.setSpout("friendscount", new FriendCountSpout());
        
        builder.setBolt("selector", new SelectorBolt()).shuffleGrouping("twitter")
        			.shuffleGrouping("hashtags")
        			.shuffleGrouping("friendscount");
        
        builder.setBolt("wordsplitter", new WordSplitterBolt(1), 8).shuffleGrouping("selector");

        builder.setBolt("filter", new IgnoreWordsBolt(), 2)
        					.fieldsGrouping("wordsplitter", new Fields("word"));
        
        builder.setBolt("counter", new RollingCountWriteBolt(30, 30))
							.fieldsGrouping("filter", new Fields("word"));
      
     }
    
	public void runLocally() throws InterruptedException {
	    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
        System.out.println("end!!!!!!");
	  }

	  public void runRemotely() throws Exception {
	    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
	  }
	  
	  
    public static void main(String[] args) throws Exception {
        
    	String topologyName = "PartC2";
        
        boolean runLocally = true;
        if (args.length >= 1 && args[0].equalsIgnoreCase("remote")) {
          runLocally = false;
        }
                
        System.out.println("Topology name: " + topologyName);
        PartC2 rtw = new PartC2();
        if (runLocally) {
          System.out.println("Running in local mode");
          rtw.runLocally();
        }
        else {
    	  System.out.println("Running in remote (cluster) mode");
          rtw.runRemotely();
        }
        
    }
}
