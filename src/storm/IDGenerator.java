package storm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class IDGenerator implements Runnable{
	private static final long BASE = 76561197960265729L;
	private static final int SPACE = (int) 176E6;
	public static final int NUM_QUERIES = 704;
	public static final int PORT = 3000;
	
	private ConcurrentLinkedQueue<Long> newBlacklistIds;
	private ConcurrentLinkedQueue<String> query_ids_strings;
	private ReentrantLock fileLock;
	private HashSet<Long> blacklist;
	private HashSet<Long> ids_generated;
//	private int idsGeneratedTotal;
	private int numQueryStringsGeneratedIteration;
	private AtomicInteger queryIteration;
	private AtomicBoolean continueExecuting;
	private Random rand;
	private AcceptBlacklistConnectionTask acceptConnectionsTask;
	private ExecutorService execService;
	
	public IDGenerator(ConcurrentLinkedQueue<String> query_string_ids, ExecutorService execService, int timeoutMillis, AtomicInteger queryIteration){
		continueExecuting = new AtomicBoolean(true);
		numQueryStringsGeneratedIteration = 0;
//		idsGeneratedTotal = 0;
		this.queryIteration = queryIteration;
		fileLock = new ReentrantLock();
		rand = new Random(System.currentTimeMillis());
		newBlacklistIds = new ConcurrentLinkedQueue<Long>();
		this.query_ids_strings = query_string_ids;
		acceptConnectionsTask = new AcceptBlacklistConnectionTask(timeoutMillis);
		this.execService = execService;
		
		ids_generated = new HashSet<Long>();
		blacklist = new HashSet<Long>();
		
		
	}
	
	private class AcceptBlacklistConnectionTask implements Runnable{
		private AtomicBoolean continueToAccept;
		private int timeoutMillis;
		
		public AcceptBlacklistConnectionTask(int timeoutMillis){
			this.timeoutMillis = timeoutMillis;
			continueToAccept = new AtomicBoolean(true);
		}
		
		@Override
		public void run() {
			System.out.println("Started id generator thread\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
			ServerSocket receiveSocket = null;
			Socket connectionSocket;
			try {
				receiveSocket = new ServerSocket(PORT);
				while(continueToAccept.get()){
					receiveSocket.setSoTimeout(timeoutMillis);
					try {
						connectionSocket = receiveSocket.accept();
						createReceiveThread(connectionSocket);
					} catch(SocketTimeoutException e) {
						//this is ok
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally{
				if(receiveSocket != null){
					try {
						receiveSocket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		public void shutdown(){
			continueToAccept.set(false);
		}

		private void createReceiveThread(Socket connectionSocket) {
			ProcessBlacklistConnectionTask currentConnection = new ProcessBlacklistConnectionTask(connectionSocket);
			execService.execute(currentConnection);
		}
		
	}
	
	private class ProcessBlacklistConnectionTask implements Runnable{
		private Socket connection;
		
		public ProcessBlacklistConnectionTask(Socket connection){
			this.connection = connection;
		}
		
		@Override
		public void run() {
			InputStream is = null;
			BufferedReader br = null;
			try {
				is = connection.getInputStream();
				br = new BufferedReader(new InputStreamReader(is));
				String line;
				while((line=br.readLine()) != null){
					newBlacklistIds.add(Long.parseLong(line));
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally{
				if(br != null){
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if(is != null){
					try {
						is.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				try {
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

	@Override
	public void run() {
		System.out.println("IDGenerator run method");
		execService.execute(acceptConnectionsTask);
		
		//continue while topology is up
		String currentQueryString = "";
		int currentQueryCount = 0;
		System.out.println("before read blacklist");
		readFromBlacklistFile();
		System.out.println("after read blacklist");
		while(continueExecuting.get()){
			System.out.println("IDGenerator run method ------------------- while loop");
			//read any newly received blacklisted ids off of queue into set
			processBlacklistIds();
			
			//if have generated 704 ids, reset the set holding generated ids 
			//to reset the iteration
			if(numQueryStringsGeneratedIteration >= NUM_QUERIES){
				numQueryStringsGeneratedIteration = 0;
				queryIteration.getAndIncrement();
				ids_generated.clear();
			}
			
			if(currentQueryCount >= 100){
				currentQueryString = currentQueryString.substring(0, currentQueryString.length()-1);
				query_ids_strings.add(currentQueryString);
				currentQueryString = "";
				currentQueryCount = 0;
				numQueryStringsGeneratedIteration++;
			}
			
			//generate enough ids to perform 704 queries with 100 ids per query
			//generate a random id not in the set and blacklist
			long nextId = generateRandomId();
			System.out.println("Random id: "+nextId);
			//add to set of ids and current string of ids
			currentQueryString += nextId;
			currentQueryString += ",";
			currentQueryCount++;
//			idsGeneratedTotal++;
			
		}
	}
	
	private void processBlacklistIds() {
		if(!newBlacklistIds.isEmpty()) {
			fileLock.lock();
			try{
				File file = new File("blacklist.txt");
				if(!file.exists()){
//					System.out.println("created file--------------------------------------------------------------\n\n\n\n\n\n\n\n\n\n");
					file.createNewFile();
				}
//				System.out.println("found file---------------------------------------------------------------------\n\n\n\n\n\n\n\n\n\n");
				FileOutputStream fs = null;
				BufferedWriter br = null;
				try {
					fs = new FileOutputStream(file, true);
					br = new BufferedWriter(new OutputStreamWriter(fs));
					while(!newBlacklistIds.isEmpty()) {
						long blacklistedId = newBlacklistIds.poll();
						boolean added = blacklist.add(blacklistedId);
						if(added) {
							br.write(blacklistedId + "\n");
						}
					}
					br.flush();
					
					fs.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if(br != null) {
						br.close();
					}
					if(fs != null) {
						fs.close();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally{
				fileLock.unlock();
			}	
		}
	}

	public void shutdown() {
		acceptConnectionsTask.shutdown();
		continueExecuting.set(false);
	}

	private void readFromBlacklistFile() {
		File file = new File("blacklist.txt");
		if(file.exists()){
			fileLock.lock();
			try {
				FileInputStream fs = null;
				BufferedReader br = null;
				try {
					String line;
					fs = new FileInputStream(file);
					br = new BufferedReader(new InputStreamReader(fs));
					while((line = br.readLine()) != null){
						long badId = Long.parseLong(line);
						blacklist.add(badId);
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if(br != null) {
						br.close();
					}
					if(fs != null) {
						fs.close();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}finally{
				fileLock.unlock();
			}
		}
	}
	
	private long generateRandomId() {
		Long nextLong;
		int next;
		do{
			next = rand.nextInt(SPACE);
			nextLong = next + BASE;
		} while(ids_generated.contains(nextLong) && !blacklist.contains(nextLong));
		ids_generated.add(nextLong);
		return nextLong;
	}
}

