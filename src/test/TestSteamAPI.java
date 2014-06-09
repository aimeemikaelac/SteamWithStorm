package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.Random;


public class TestSteamAPI {
	private static final String key = "10FD1E43E733E34BCACA0DD36CFEB899"; 
	private static final long BASE = 76561197960265729L;
	private static final int SPACE = (int) 176E6;
	
	private static LinkedList<Long> idsToScan = new LinkedList<Long>();
	private static LinkedList<Long> idsBlacklist = new LinkedList<Long>();
	private static Random rand = new Random(System.currentTimeMillis());
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String requestUrl = "http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=" + key + "&steamids=";
		
		String ids = generateIds();
		
		requestUrl += ids;
		
		requestUrl += "&format=json";
		
		URL url;
		InputStream is = null;
		BufferedReader br;
		String line;
		
		try {
			url = new URL(requestUrl);
			is = url.openStream();
			br = new BufferedReader(new InputStreamReader(is));
			
			while((line = br.readLine()) != null){
				System.out.println(line);
			} 
			
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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

	}
	private static String generateIds() {
		String idsList = "";
		for(int i = 0; i<100; i++){
			generateRandomId();
		}
		for(long id : idsToScan){
			idsList += id;
			idsList += ",";
		}
		idsList.substring(0, idsList.length()-1);
		return idsList;
	}
	
	private static void generateRandomId(){
//		Random rand = new Random(System.currentTimeMillis());
		Long nextLong;
		int next;
		do{
			next = rand.nextInt(SPACE);
			nextLong = next + BASE;
		} while(idsToScan.contains(nextLong) && !idsBlacklist.contains(nextLong));
		idsToScan.add(nextLong);
	}

}
