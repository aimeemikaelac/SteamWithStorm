package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class PageCrawlTest {
	private static final long BASE = 76561197960265729L;
	private static final int SPACE = (int) 176E6;
	private static LinkedList<Long> idsToScan = new LinkedList<Long>();
	private static LinkedList<Long> idsBlacklist = new LinkedList<Long>();
	private static int online = 0;
	private static int offline = 0;
	private static Random rand = new Random(System.currentTimeMillis());
	private static enum ProfileState{
		ONLINE,OFFLINE,INVALID;
	}
	
	public static void main(String[] args){
		if(args.length != 1){
			System.out.println("need to have the number of samples");
			return;
		}
		int numSamples = Integer.parseInt(args[0]);
		Long startRand;
		Long startCrawl;
		Long endRand;
		Long endCrawl;
		startRand = System.nanoTime();
		generateSampleSet(numSamples);
		endRand = System.nanoTime();
		System.out.println("Random numbers generated in: "+TimeUnit.NANOSECONDS.toMillis(endRand-startRand) + " ms");
		startCrawl = System.nanoTime();
		crawlSteamPages();
		endCrawl = System.nanoTime();
		System.out.println("Crawl time: "+TimeUnit.NANOSECONDS.toSeconds(endCrawl - startCrawl) + " s");
		System.out.println("Random numbers generated in: "+TimeUnit.NANOSECONDS.toMillis(endRand-startRand) + " ms");
		System.out.println("Runtime: "+TimeUnit.NANOSECONDS.toMillis(endCrawl-startRand) + " s");
		System.out.println("Users online: "+online);
		System.out.println("Users offline: "+offline);
	}
	
	private static void generateSampleSet(int numSamples){
		for(int i = 0; i<numSamples; i++){
			generateRandomId();
		}
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
	
	private static void crawlSteamPages(){
		String page;
		ProfileState state;
		Collections.sort(idsToScan);
		while(!idsToScan.isEmpty()){
			long id = idsToScan.getFirst();
			page = fetchSteamPage(id);
			if(page.length() > 0){
				state = parseSteamPage(page);
				switch(state){
					case ONLINE:
						System.out.println("Id "+id+" is online-------------");
						online++;
						break;
					case OFFLINE:
						System.out.println("Id "+id+" is offline++++++++++++");
						offline++;
						break;
					case INVALID:
//						System.out.println("Could not fetch page for id: "+id);
						idsBlacklist.add(id);
						generateRandomId();
						Collections.sort(idsToScan);
						break;
				}
			}
			idsToScan.removeFirst();
		}
	}
	
	private static ProfileState parseSteamPage(String page) {
		if(page.length() == 0){
			throw new IllegalArgumentException("Page length must be non-zero");
		}
		boolean isOnline = page.contains("Currently Online");
		boolean isOffline = page.contains("Currently Offline");
		if(isOnline && isOffline){
			throw new IllegalStateException("User cannot be online and offline at the same time");
		}
		if(!isOnline && !isOffline){
			return ProfileState.INVALID;
		} else if(isOnline){
			return ProfileState.ONLINE;
		} else{
			return ProfileState.OFFLINE;
		}
	}

	private static String fetchSteamPage(long id){
		URL url;
		InputStream is = null;
		BufferedReader br;
		String line;
		String page = "";
		
		try {
			url = new URL("http://steamcommunity.com/profiles/"+id);
			is = url.openStream();
			br = new BufferedReader(new InputStreamReader(is));
			
			while((line = br.readLine()) != null){
				page += (line + "\n");
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
		return page;
	}
}
