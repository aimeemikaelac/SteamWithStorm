package storm;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
public class BlacklistWriter extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static int totalBadIds = 0;
	
	@Override
	public void execute(Tuple input) {
		ArrayList<Object> values = (ArrayList<Object>) input.getValues();
		String badIdsString = (String) values.get(0);
		String[] badIds = badIdsString.split(",");
		String ip_address = (String) input.getValueByField("ip_address");
		
		writeBadIdsToSocket(badIds, ip_address); 
		
	}

	private void writeBadIdsToSocket(String[] badIds, String ip_address) {
		Socket socket = null;
		OutputStream os = null;
		BufferedWriter br = null;
		try {
			try {
				socket = new Socket(ip_address, IDGenerator.PORT);
				os = socket.getOutputStream();
				br = new BufferedWriter(new OutputStreamWriter(os));
				for(String badId : badIds) {
					br.write(badId + "\n");
				}
				br.flush();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			finally {
				if(br != null) {
					br.close();
				}
				if(os != null) {
					os.close();
				}
				if(socket != null) {
					socket.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
	}

}
