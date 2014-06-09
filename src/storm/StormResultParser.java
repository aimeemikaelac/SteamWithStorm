package storm;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class StormResultParser extends BaseBasicBolt {

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		int numOnline = 0;
		int numOffline = 0;
		HashSet<Long> badIds = new HashSet<Long>();
		ArrayList<Object> values = (ArrayList<Object>) input.getValues();
		String resultString = (String) values.get(0);
		String iteration = (String) values.get(1);
		JSONParser parser = new JSONParser();
		try {
			JSONObject object = (JSONObject) parser.parse(resultString);
			JSONObject response = (JSONObject) object.get("response");
			JSONArray players = (JSONArray) response.get("players");
			for(Object playerObj : players){
				JSONObject player = (JSONObject) playerObj;
				String idString = (String) player.get("steamid");
				if(player.containsKey("profilestate")){
//					String profilestate = ((Long)player.get("profilestate")).toString();
					Long profilestate = (Long) player.get("profilestate");
					if(profilestate == 1){
//						String personastateString = (String) player.get("personastate");
//						int personastate = Integer.parseInt(personastateString);
						Long personastate = (Long) player.get("personastate");
						if(personastate == 0){
							numOffline++;
						} else{
							numOnline++;
						}
					} 
				} else{
					long id = Long.parseLong(idString);
					badIds.add(id);
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String badIdsString = "";
		if(!badIds.isEmpty()){
			for(long id : badIds){
				badIdsString += id;
				badIdsString += ",";
			}
			badIdsString = badIdsString.substring(0, badIdsString.length()-1);
		}
		List<Object> tuple = new ArrayList<Object>();
		tuple.add(new Integer(numOnline).toString());
		tuple.add(new Integer(numOffline).toString());
		tuple.add(badIdsString);
		tuple.add(iteration);
		collector.emit(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fields = new ArrayList<String>();
		fields.add("numOnline");
		fields.add("numOffline");
		fields.add("badIds");
		fields.add("iteration");
		declarer.declare(new Fields(fields));
	}

}
