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
package org.apache.storm.starter.bolt;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.apache.log4j.Logger;
import org.apache.storm.starter.tools.NthLastModifiedTimeTracker;
import org.apache.storm.starter.tools.SlidingWindowCounter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class RollingCountWriteBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8345855542568233417L;

	private static final Logger logger = Logger.getLogger(RollingCountWriteBolt.class);
    
	/** Number of seconds before the top list will be logged to stdout. */
    private final long logIntervalSec;
    
    /** Number of seconds before the top list will be cleared. */
    private final long clearIntervalSec;
    
    /** Number of top words to store in stats. */
    private int topListSize = 1;

    private Map<String, Long> counter;
    private Map<Long,String> tweets= new HashMap<Long,String>();
    private long lastLogTime;
    private long lastClearTime;

    public RollingCountWriteBolt(long logIntervalSec, long clearIntervalSec) {
        this.logIntervalSec = logIntervalSec;
        this.clearIntervalSec = clearIntervalSec;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        counter = new HashMap<String, Long>();
        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple input) {
    	Long id = input.getLongByField("id");
        if (!tweets.containsKey(id)) {
        	tweets.put(id, input.getStringByField("text"));
        }
        String word = input.getStringByField("word");
        
        Long count = counter.get(word);
        count = count == null ? 1L : count + 1;
        counter.put(word, count);

//        logger.info(new StringBuilder(word).append('>').append(count).toString());

        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {
        	logger.info("\n\n");
        	logger.info("Word count: "+counter.size());

            try {
				publishTopList();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            lastLogTime = now;
        }
    }

    private void publishTopList() throws FileNotFoundException {
        long timestamp = System.currentTimeMillis();

        System.out.println(tweets.size());
        OutputStream t = new FileOutputStream("PartCQ2_results/tweets"+timestamp+".txt", true);
        for (Map.Entry<Long, String> entry : tweets.entrySet()) {
        	try {
				t.write(entry.getValue().getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//            logger.info(new StringBuilder("top - ").append(entry.getValue()).append('|').append(entry.getKey()).toString());
        }
        try {
			t.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			t.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        // calculate top list:
    	topListSize = counter.size()/2;
        SortedMap<Long, String> top = new TreeMap<Long, String>();
        for (Map.Entry<String, Long> entry : counter.entrySet()) {
            long count = entry.getValue();
            String word = entry.getKey();

            top.put(count, word);
            if (top.size() > topListSize) {
                top.remove(top.firstKey());
            }
        }
        
        // Output top list:
        OutputStream o = new FileOutputStream("PartCQ2_results/top"+timestamp+".txt", true);
        
        for (Map.Entry<Long, String> entry : top.entrySet()) {
        	try {
				o.write(new StringBuilder(entry.getValue()).append(':')
								.append(entry.getKey()).append("\n").toString().getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//            logger.info(new StringBuilder("top - ").append(entry.getValue()).append('|').append(entry.getKey()).toString());
        }
        
        try {
			o.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			o.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        
 

     // Clear top list
        long now = System.currentTimeMillis();
        if (now - lastClearTime > clearIntervalSec * 1000) {
            counter.clear();
            tweets.clear();
            lastClearTime = now;
        }
    }
}
