package uk.ac.imperial.lsds.seepworker.comm;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.InputAdapter;
import uk.ac.imperial.lsds.seepworker.core.input.NetworkDataStream;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;

public class BasicWorkerWorkerCommunicationTest {

//	@Test
//	public void test() {
//		// Create inputAdapter map that is used to configure networkselector
//		int clientId = 100;
//		Schema s = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
//		Map<Integer, InputAdapter> iapMap = null;
//		iapMap = new HashMap<>();
//		Properties p = new Properties();
//		p.setProperty("master.ip", "127.0.0.1");
//		NetworkDataStream nds = new NetworkDataStream(new WorkerConfig(p), clientId, s, null);
//		iapMap.put(clientId, nds);
//		// TODO: build this
//		NetworkSelector ds = new NetworkSelector(iapMap);
//		// Create client and server that will be interchanging data
//		InetAddress myIp = null;
//		try {
//			myIp = InetAddress.getByName("127.0.0.1");
//		} 
//		catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
//		int listeningPort = 5555;
//		ds.configureAccept(myIp, listeningPort);
//		
//		// create outputbuffer for the client
//		Connection c = new Connection(new EndPoint(clientId, myIp, listeningPort));
//		OutputBuffer ob = new OutputBuffer(null, clientId, c);
//		Set<OutputBuffer> obs = new HashSet<>();
//		obs.add(ob);
//		ds.configureConnect(obs);
//		
//		ds.start();
//		
//		try {
//			Thread.sleep(1000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		//((EventAPI)ds).readyForWrite(clientId);
//		
//		
//		while(true){
//			try {
//				Thread.sleep(1000);
//			} 
//			catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//		
//		
//		
//		// start conn initiation, sending ids, etc
//		
//		//assertTrue(true);
//		
//	}
	
	@Test
	public void testSendTuples() {
		// Create inputAdapter map that is used to configure networkselector
		int clientId = 100;
		int streamId = 101;
		Schema s = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		Map<Integer, InputAdapter> iapMap = null;
		iapMap = new HashMap<>();
		Properties p = new Properties();
		p.setProperty("master.ip", "127.0.0.1");
		p.setProperty("batch.size", "10");
		WorkerConfig fake = new WorkerConfig(p);
		NetworkDataStream nds = new NetworkDataStream(new WorkerConfig(p), clientId, s, null);
		iapMap.put(clientId, nds);
		// TODO: build this
		NetworkSelector ds = NetworkSelector.makeNetworkSelectorWithMap(iapMap);
		// Create client and server that will be interchanging data
		InetAddress myIp = null;
		try {
			myIp = InetAddress.getByName("127.0.0.1");
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		int listeningPort = 5555;
		ds.configureAccept(myIp, listeningPort);
		
		// create outputbuffer for the client
		Connection c = new Connection(new EndPoint(clientId, myIp, listeningPort));
		OutputBuffer ob = new OutputBuffer(fake, clientId, c, streamId);
		Set<OutputBuffer> obs = new HashSet<>();
		obs.add(ob);
		ds.configureConnect(obs);
		
		ds.initNetworkSelector();
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		/** 1 send **/
		
		// Create tuple and send it to the other worker
		byte[] serializedData = OTuple.create(s, new String[]{"userId", "ts"}, new Object[]{3, 23423L});
		//byte[] serializedData2 = OTuple.create(s, new String[]{"userId", "ts"}, new Object[]{4, 8384L});
		System.out.println("Tuple length: "+serializedData.length);
		//ob.write(serializedData);
		boolean canSend = ob.write(serializedData);
		if(canSend){
			System.out.println("Notifying to send");
			((EventAPI)ds).readyForWrite(clientId);
		} else{
			System.out.println("CANNOT send yet");
		}
		
		ITuple incomingTuple = nds.pullDataItem(500); // blocking until there's something to receive
		System.out.println(incomingTuple.toString());
		
//		ITuple incomingTuple2 = nds.pullDataItem(); // blocking until there's something to receive
//		System.out.println(incomingTuple2.toString());
		
		/** 2 send **/
		// Create tuple and send it to the other worker
		byte[] serializedData2 = OTuple.create(s, new String[]{"userId", "ts"}, new Object[]{4, 848448L});
		System.out.println("Tuple length: "+serializedData2.length);
		boolean canSend2 = ob.write(serializedData2);
		if(canSend2){
			System.out.println("Notifying to send");
			((EventAPI)ds).readyForWrite(clientId);
		} else{
			System.out.println("CANNOT send yet");
		}
		
		ITuple incomingTuple2 = nds.pullDataItem(500); // blocking until there's something to receive
		System.out.println(incomingTuple2.toString());
		
		ITuple incomingTuple3 = nds.pullDataItem(500); // blocking until there's something to receive
		System.out.println(incomingTuple3.toString());
		
		while(true){
			try {
				Thread.sleep(1000);
			} 
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public void basicSocketCommTest(){
		
	}

}
