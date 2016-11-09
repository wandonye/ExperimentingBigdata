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
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import org.apache.storm.starter.bolt.WriteBolt;
import org.apache.storm.starter.spout.TwitterSampleSpout;

public class PartC {        
	
    public static void main(String[] args) {
        String consumerKey = "phhCqmJBYarJE2kX74oVT6L1m"; 
        String consumerSecret = "n9WjhY81IFi3zy0qL69YxDnwkAYuamaaMINcbt7RBbOGUC1ObV"; 
        String accessToken = "56472627-tepJv0DiQOniiUxVcDbh7gDfr7TxWFjDkeoJ4L6ht"; 
        String accessTokenSecret = "3C3MIXeJCdVThTIgooiwqGF6iGgD4lUsKnx0KMVLWJGB7";
        String[] keyWords = {"Trump","president","election","Hillary",
        		"market","marathon","breaking", "news","FBI", 
        		"Google", "Apple", "O2O","Uber", "Elon Musk", "Tesla",
        		"AI", "TED", "stock","technology","education","cat","obama",
        		"robot","IoT", "hollywood"};

//        String consumerKey = args[0]; 
//        String consumerSecret = args[1]; 
//        String accessToken = args[2]; 
//        String accessTokenSecret = args[3];
//        String[] arguments = args.clone();
//        String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("twitter", new TwitterSampleSpout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, keyWords));
        builder.setBolt("print", new WriteBolt())
                .shuffleGrouping("twitter");
                
                
        Config conf = new Config();
        //conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 2);
        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("PartC1", conf, builder.createTopology());
        
        Utils.sleep(20000000);
        cluster.shutdown();
        System.out.println("end!!!!!!");
    }
}
