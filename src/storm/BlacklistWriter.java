package storm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
public class BlacklistWriter extends BaseRichBolt {
	public static int totalBadIds = 0;
	
	@Override
	public void execute(Tuple input) {
		ArrayList<Object> values = (ArrayList<Object>) input.getValues();
		String badIdsString = (String) values.get(0);
		String[] badIds = badIdsString.split(",");
		//spin until gui is launched
//		while(CounterBolt.gui == null){}
//		for(String badId : badIds){
//			IDGeneratorSpout.blacklist.add(Long.parseLong(badId));
//			totalBadIds++;
//			CounterBolt.gui.updateInvalidPane(totalBadIds, badId);
//		}
		IDGeneratorSpout.fileLock.lock();
		try{
			File file = new File("/home/michael/blacklist.txt");
			if(!file.exists()){
				System.out.println("created file--------------------------------------------------------------\n\n\n\n\n\n\n\n\n\n");
				file.createNewFile();
			}
			System.out.println("found file---------------------------------------------------------------------\n\n\n\n\n\n\n\n\n\n");
			FileOutputStream fs;
			try {
				fs = new FileOutputStream(file, true);
				BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs));
				for(String badId : badIds){
					br.write(badId + "\n");
				}
				br.flush();
				br.close();
				fs.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			IDGeneratorSpout.fileLock.unlock();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		if(IDGeneratorSpout.fileLock == null){
			IDGeneratorSpout.fileLock = new ReentrantLock();
		}
		
	}

}
