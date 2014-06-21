package storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class CounterBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int currentIteration = 0;
	int totalOnline = 0;
	int totalOffline = 0;
	int totalInvalid = 0;
	int totalQueries = 0;
	int numQueries = 0;
	OutputCollector collector;
	public static SteamGui gui = null;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		System.out.println("Created gui\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
		log("created gui\n");
		createGui();
		this.collector = collector;
	}
	
	public static void log(String message){
		/*File file = new File("/home/michael/log.txt");
		if(!file.exists()){
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			FileOutputStream fs = new FileOutputStream(file, true);
			BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fs));
			wr.write(message);
			wr.flush();
			wr.close();
			fs.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}
	private void createGui() {
		gui = new SteamGui();
		gui.setIteration(currentIteration);
	}

	@Override
	public void execute(Tuple input) {
		List<Object> values = input.getValues();
		int numOnline = Integer.parseInt((String)values.get(0));
		int numOffline = Integer.parseInt((String)values.get(1));
		String badIdsString = (String)values.get(2);
		int iteration = Integer.parseInt((String)values.get(3));
		List<Object> tuple = new ArrayList<Object>();
		if(iteration == currentIteration){
			totalOnline += numOnline;
			totalOffline += numOffline;
			numQueries++;
//			updateGui();
			
			updateGuiPercentage(numQueries);
			
		} else if(iteration > currentIteration){
			currentIteration = iteration;
			numQueries = 0;
			updateGui();
			totalOnline = 0;
			totalOffline = 0;
		}
//		updateInvalidIds(badIdsString);
		totalQueries++;
		updateQueryCount(totalQueries);
		tuple.add(badIdsString);
		tuple.add(input.getValueByField("ip_address"));
		collector.emit(tuple);
	}
	
//	private void updateInvalidIds(String badIdsString) {
//		String[] badIds = badIdsString.split(",");
//		for(String badId : badIds){
//			totalInvalid++;
//			gui.updateInvalidPane(totalInvalid, badId);
//		}
//	}

	private void updateQueryCount(int numQueries){
		gui.updateQueryPane(numQueries);
	}

	private void updateGuiPercentage(int numQueries2) {
		log("update gui\n");
		System.out.println("Updated gui\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
		double percentage = (double) numQueries / (double) IDGenerator.NUM_QUERIES * 100.0;
		gui.updateProgressBar(percentage);
	}

	private void updateGui() {
		log("update gui\n");
		System.out.println("Updated gui\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
		int totalSampledUsers = totalOnline + totalOffline;
		double percentageOnline = (double) totalOnline / (double) totalSampledUsers;
		double totalUsers = 76E6;
		double totalOnline = totalUsers*percentageOnline;
		gui.addDataPoint(System.currentTimeMillis(), (int) totalOnline);
		gui.updateProgressBar(0);
		gui.setIteration(currentIteration);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fields = new ArrayList<String>();
		fields.add("badIds");
		fields.add("ip_address");
		declarer.declare(new Fields(fields));
	}
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException{
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("id_generator", new IDGeneratorSpout(), 1);
		
		builder.setBolt("requester", new SteamQueryBolt(), 3).shuffleGrouping("id_generator");
		
		builder.setBolt("parser", new StormResultParserBolt(), 3).shuffleGrouping("requester");
		
		builder.setBolt("counter", new CounterBolt(), 1).shuffleGrouping("parser");
		
		builder.setBolt("blacklister", new BlacklistWriter(), 1).shuffleGrouping("counter");
		
		Config conf = new Config();
		conf.setNumWorkers(9);
//		conf.setDebug(true);
		
		StormSubmitter.submitTopology("steam_topology", conf, builder.createTopology());
	}

	
}
