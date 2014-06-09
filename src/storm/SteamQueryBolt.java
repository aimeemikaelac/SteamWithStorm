package storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SteamQueryBolt extends BaseBasicBolt {
	private static final String key = "10FD1E43E733E34BCACA0DD36CFEB899"; 
	private final String baseUrl = "http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=10FD1E43E733E34BCACA0DD36CFEB899&steamids=";
	/* example JSON:
	 * http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=10FD1E43E733E34BCACA0DD36CFEB899&steamids=76561197970347865&format=json
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fields = new ArrayList<String>();
		fields.add("result");
		fields.add("iteration");
		declarer.declare(new Fields(fields));
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		ArrayList<Object> values = (ArrayList<Object>) input.getValues();
		String currentRequest = baseUrl;
		String ids = (String) values.get(0);
		String iteration = (String) values.get(1);
		currentRequest += ids;
		currentRequest += "&format=json";
		URL url;
		InputStream is = null;
		BufferedReader br;
		String line;
		String result = "";
		try {
			url = new URL(currentRequest);
			is = url.openStream();
			br = new BufferedReader(new InputStreamReader(is));
			
			while((line = br.readLine()) != null){
				result += line;
				line += "\n";
			} 
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			if(is != null){
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		List<Object> tuple = new ArrayList<Object>();
		tuple.add(result);
		tuple.add(iteration);
		collector.emit(tuple);
	}

}
