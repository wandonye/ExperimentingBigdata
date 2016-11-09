/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.starter.spout;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This spout generates random whole numbers with given {@code maxNumber} value as maximum with the given {@code fields}.
 *
 */
public class RandomHashTagsSpout extends BaseRichSpout {
    /**
	 * 
	 */
	private static final long serialVersionUID = 539479408892989162L;
    private final int maxNumber;
    private final Map<Long, List<List<Object>>> batches = new HashMap<>();
    private final String[] hashTags = {"HillaryForPrison","OBAMA","Hollywood","Wikileaks",
    		"WIN","fashion","Trump","smartphones","celebs","news","retail","online","ecommerce",
    		"VoteTrump","Election2016","socialmedia","ElectionFinalThoughts","DLFforum",
    		"failchat","NFL","ImVotingBecause","BUFvsSEA","Bills","hottest","hiring",
    		"tweetmyjobs","jobposting","opportunity","jobsearch","needajob","interview","howhot",
    		"love","photooftheday","Beautiful","fashion", "like4like",
    		"picoftheday","happy","followme", "art","follow","style","cute","life",
    		"fun","girl","nature","amazing","travel","smile","emabiggestfansjustinbieber","auspol", "ausvsa",
    		"xfactorau","emabiggestfansladygaga", "vote","imwithher","nbc4dc","debates","mannequinchallenge",
    		"thatssoraven","disney","mondaymotivation","coffee","starwars","quran","hadith","gfvip","quote",
    		"maga","rt","soundcloud","win","periscope","isangpamilyatayo","gameinsight","free","election2016",
    		"amas","news","trecru","nowplaying","android","androidgames","facebook"
    		};
    
    private static final Logger LOG = LoggerFactory.getLogger(RandomIntegerSpout.class);
    private SpoutOutputCollector collector;
    private Random rand;

    public RandomHashTagsSpout() {
        this.maxNumber = hashTags.length;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tags"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        String tagString = "";
        int numOfBatch = rand.nextInt(this.maxNumber-10)+10;
        Set<Integer> tagIndexes = new TreeSet<Integer>();
        for (int i=0;i<numOfBatch;i++){
        	int newIndex = rand.nextInt(numOfBatch);
        	if(tagIndexes.add(newIndex)){
        		tagString += "#"+ hashTags[newIndex];
        	}
        }
        
        collector.emit(new Values(tagString));
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Got ACK : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Got FAIL : " + msgId);
    }
    
}
