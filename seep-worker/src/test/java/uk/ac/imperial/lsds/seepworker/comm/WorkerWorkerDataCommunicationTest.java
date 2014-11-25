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

public class WorkerWorkerDataCommunicationTest {

	@Test
	public void test() {
		// Create inputAdapter map that is used to configure networkselector
		int clientId = 100;
		int streamId = 101;
		Schema s = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		Map<Integer, InputAdapter> iapMap = null;
		iapMap = new HashMap<>();
		Properties p = new Properties();
		p.setProperty("master.ip", "127.0.0.1");
		p.setProperty("batch.size", "1000"); // 25 - 25 - 400
		p.setProperty("rx.buffer.size", "10000"); // 66 - 116 - 817
		p.setProperty("tx.buffer.size", "10000"); // 66 - 116 - 817
		WorkerConfig fake = new WorkerConfig(p);
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
		OutputBuffer ob = new OutputBuffer(fake, clientId, c, streamId);
		Set<OutputBuffer> obs = new HashSet<>();
		obs.add(ob);
		ds.configureConnect(obs);
		
		ds.start();
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		/** Continuous sending **/
		int interWriteTime = -1;
		Writer w = new Writer(clientId, ob, ds, interWriteTime);
		Thread writer = new Thread(w);
		writer.setName("ImTheWriter");
		
		Reader r = new Reader(nds);
		Thread reader = new Thread(r);
		reader.setName("ImTheReader");
		
		reader.start();
		writer.start();
		
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
	
	class Writer implements Runnable{

		int clientId;
		OutputBuffer ob;
		NetworkSelector ds;
		int sleep;
		
		public Writer(int clientId, OutputBuffer ob, NetworkSelector ds, int sleep){
			this.clientId = clientId;
			this.ob = ob;
			this.ds = ds;
			this.sleep = sleep;
		}
		
		@Override
		public void run() {
			Schema s = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
			int userId = 0;
			long ts = System.currentTimeMillis();
			while(true){
				ts = System.currentTimeMillis();
				userId++;
				byte[] serializedData = OTuple.create(s, new String[]{"userId", "ts"}, new Object[]{userId, ts});
				boolean canSend = ob.write(serializedData);
				if(canSend){
					((EventAPI)ds).readyForWrite(clientId);
				}
				if(sleep > -1){
					try {
						Thread.sleep(sleep);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}	
		}	
	}
	class Reader implements Runnable{
		NetworkDataStream nds;
		public Reader(NetworkDataStream nds){
			this.nds = nds;
		}
		@Override
		public void run() {
			int counter = 0;
			long ts = System.currentTimeMillis();
			while(true){
				ITuple incomingTuple = nds.pullDataItem(); // blocking until there's something to receive
				//System.out.println(incomingTuple.toString());
				counter++;
				if((System.currentTimeMillis()) - ts > 1000){
					System.out.println("e/s: "+counter);
					counter = 0;
					ts = System.currentTimeMillis();
				}
			}	
		}	
	}
}