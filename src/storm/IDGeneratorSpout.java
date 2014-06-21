package storm;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class IDGeneratorSpout extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
//	private int queriesMade;
	private int objectIds;
	private AtomicInteger queryIteration;
	private HashMap<Object, List<Object>> objectIdMap;
	private IDGenerator generator;
	private ExecutorService execService;
	private ConcurrentLinkedQueue<String> idlist;
	private String ipaddress;
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		objectIdMap = new HashMap<Object, List<Object>>();
		this.collector = collector;
//		queriesMade = 0;
		objectIds = 0;
		queryIteration = new AtomicInteger(0);
		execService = Executors.newCachedThreadPool();
		idlist = new ConcurrentLinkedQueue<String>();
		
		try {
			ipaddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		generator = new IDGenerator(idlist, execService, 2000, queryIteration);
		execService.execute(generator);
	}
	@Override
	public void nextTuple() {
		System.out.println("Spout creating tuple\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
		while(!idlist.isEmpty()) {
			String currentQuery = idlist.poll();
			
			List<Object> tuple = new ArrayList<Object>();
			tuple.add(currentQuery);
			tuple.add(new Integer(queryIteration.get()).toString());
			tuple.add(ipaddress);
			Object id = (Object)(new Integer(objectIds));
			System.out.println("Spout emitting tuple\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
			collector.emit(tuple, id);
			objectIds++;
			objectIdMap.put(id, tuple);
			
//			queriesMade++;
		}
	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fields = new ArrayList<String>();
		fields.add("id");
		fields.add("iteration");
		fields.add("ip_address");
		declarer.declare(new Fields(fields));
	}
	
	@Override
	public void close() {
		generator.shutdown();
		execService.shutdown();
		boolean shutdown = true;
		try {
			shutdown = execService.awaitTermination(20, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(!shutdown){
			System.out.println("could not shutdown executor service");
		}
	}
	

}
