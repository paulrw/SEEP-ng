package uk.ac.imperial.lsds.seepworker.comm;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

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

	@Test
	public void test() {
		// Create inputAdapter map that is used to configure networkselector
		int clientId = 100;
		Schema s = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		Map<Integer, InputAdapter> iapMap = null;
		iapMap = new HashMap<>();
		Properties p = new Properties();
		p.setProperty("master.ip", "127.0.0.1");
		NetworkDataStream nds = new NetworkDataStream(new WorkerConfig(p), clientId, s, null);
		iapMap.put(clientId, nds);
		// TODO: build this
		NetworkSelector ds = new NetworkSelector(iapMap);
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
		OutputBuffer ob = new OutputBuffer(null, clientId, c);
		Set<OutputBuffer> obs = new HashSet<>();
		obs.add(ob);
		ds.configureConnect(obs);
		
		ds.start();
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//((EventAPI)ds).readyForWrite(clientId);
		
		while(true){
			try {
				Thread.sleep(1000);
			} 
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// start conn initiation, sending ids, etc
		
		//assertTrue(true);
		
	}
	
	public void basicSocketCommTest(){
		
	}

}
