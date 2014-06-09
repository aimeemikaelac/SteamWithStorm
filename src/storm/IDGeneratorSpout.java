package storm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class IDGeneratorSpout extends BaseRichSpout {
	private static final long BASE = 76561197960265729L;
	private static final int SPACE = (int) 176E6;
	public static final int NUM_QUERIES = 704;
	private Random rand;
	private SpoutOutputCollector collector;
	private HashSet<Long> queryIds;
	public static HashSet<Long> blacklist;
	public static ReentrantLock fileLock = null;
	private int queriesMade;
	private int currentQueryCount;
	private String currentQueryString;
	private int objectIds;
	private int queryIteration;
	private HashMap<Object, List<Object>> objectIdMap;
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		rand = new Random(System.currentTimeMillis());
		queryIds = new HashSet<Long>();
		blacklist = new HashSet<Long>();
		objectIdMap = new HashMap<Object, List<Object>>();
		this.collector = collector;
		queriesMade = 0;
		currentQueryCount = 0;
		objectIds = 0;
		queryIteration = 0;
		currentQueryString = "";
		if(fileLock == null){
			fileLock = new ReentrantLock();
		}
	}
	@Override
	public void nextTuple() {
		//once 704 items have been put onto the queue, increment the sample number
		if(queriesMade == NUM_QUERIES){
			queriesMade = 0;
			queryIteration++;
			queryIds.clear();
		}
		
		//if at the start of a sampling, read from blacklist file
		if(queriesMade == 0 && blacklist.isEmpty()){
			File file = new File("~/blacklist.txt");
			if(file.exists()){
				fileLock.lock();
				try {
					String line;
					FileInputStream fs = new FileInputStream(file);
					BufferedReader br = new BufferedReader(new InputStreamReader(fs));
					while((line = br.readLine()) != null){
						long badId = Long.parseLong(line);
						blacklist.add(badId);
					}
					br.close();
					fs.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally{
					fileLock.unlock();
				}
			}
//			for(long id : blacklist){
//				BlacklistWriter.totalBadIds++;
//				CounterBolt.gui.updateInvalidPane(BlacklistWriter.totalBadIds, new Long(id).toString());
//			}
		}
		
		//once 100 ids have been generated, put onto queue with a sample number
		if(currentQueryCount >= 100){
			currentQueryString = currentQueryString.substring(0, currentQueryString.length()-1);
			List<Object> tuple = new ArrayList<Object>();
			tuple.add(currentQueryString);
			tuple.add(new Integer(queryIteration).toString());
			Object id = (Object)(new Integer(objectIds));
			collector.emit(tuple, id);
			objectIds++;
			objectIdMap.put(id, tuple);
			
			
			currentQueryString = "";
			currentQueryCount = 0;
			
			queriesMade++;
		}
		//generate enough ids to perform 704 queries with 100 ids per query
			//generate a random id not in the set and blacklist
		long nextId = generateRandomId();
			//add to set of ids and current string of ids
		currentQueryString += nextId;
		currentQueryString += ",";
		currentQueryCount++;
			
		
	}
	
	private long generateRandomId() {
		Long nextLong;
		int next;
		do{
			next = rand.nextInt(SPACE);
			nextLong = next + BASE;
		} while(queryIds.contains(nextLong) && !blacklist.contains(nextLong));
		queryIds.add(nextLong);
		return nextLong;
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fields = new ArrayList<String>();
		fields.add("id");
		fields.add("iteration");
		declarer.declare(new Fields(fields));
	}
	

}
