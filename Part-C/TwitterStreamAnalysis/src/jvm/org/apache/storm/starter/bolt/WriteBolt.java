package org.apache.storm.starter.bolt;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

public class WriteBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1546156927867403807L;
	int count = 0;
	OutputStream o;
	Map conf = Utils.readStormConfig();
	NimbusClient cc;
	Nimbus.Client client;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		conf.put("nimbus.seeds", Arrays.asList("10.254.0.142"));
		cc = NimbusClient.getConfiguredClient(conf);
		client = cc.getClient();
		try {
			o = new FileOutputStream("tweets.txt", true);
		} catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
        
        String tmp = input.getString(0);
        System.out.println(count+": "+tmp);
        tmp += "\n";
        count++;
        if (count<500000) {
        	try {
                o.write(tmp.getBytes());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }else{
        	try {
                System.out.println("Collected enough tweets. Shutting down");
				client.killTopology("PartC1");
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
