package org.apache.storm.starter.bolt;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class SelectorBolt extends BaseRichBolt {
	OutputCollector _collector;
	/**
	 * 
	 */
	private long fcount = 300000;
	private String[] givenTags = {};
	private static final long serialVersionUID = 5294679966385838651L;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
        String sourceStreamId = input.getSourceComponent();
        System.out.println("==========================");
    	System.out.println(sourceStreamId);
        System.out.println("==========================");
        if (sourceStreamId.equals("friendscount")){
        	fcount = input.getIntegerByField("fcount");
        }else if (sourceStreamId.equals("hashtags")) {
        	String tagStr = input.getStringByField("tags");
            givenTags = tagStr.split("#");
        }else if (sourceStreamId.equals("twitter")){
        	Status tweet = (Status) input.getValueByField("tweet");
            
        	System.out.println(givenTags.toString());
        	System.out.println("vs");
        	System.out.println(tweet.getHashtagEntities().toString());
            boolean hasTag = false;
            for (HashtagEntity tag:tweet.getHashtagEntities()){
            	if(Arrays.asList(givenTags).contains(tag.getText())){
            		hasTag = true;
            		break;
            	}
            }
            if (hasTag && tweet.getUser().getFriendsCount()<fcount){
                _collector.emit(new Values(tweet.getId(),tweet.getText()));
            }
        }
        
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id","text"));
	}

}
