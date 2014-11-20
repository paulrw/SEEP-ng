package uk.ac.imperial.lsds.seepworker.core.output;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class OutputBuffer {

	private int opId;
	private Connection c;
	
	private ByteBuffer buf;
	
	public OutputBuffer(WorkerConfig wc, int opId, Connection c){
		this.opId = opId;
		this.c = c;
		int batchSize = wc.getInt(WorkerConfig.BATCH_SIZE);
		buf = ByteBuffer.allocate(batchSize + OutputAdapter.PER_TUPLE_OVERHEAD_SIZE);
	}
	
	public int id(){
		return opId;
	}
	
	public Connection getConnection(){
		return c;
	}

	public boolean write(byte[] data){
		while(buf.remaining() < data.length){
			// If there is not space enough, then block until there is
			waitForSpace();
		}
		synchronized(buf){
			buf.put(data);
		}
		// Assumes that all tuples are similar sized...
		if(buf.remaining() < data.length){
			return true;
		}
		return false;
	}
	
	private void waitForSpace(){
		try {
			synchronized(this){
				this.wait();
			}
		}
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void notifyOfSpace(){
		synchronized(this){
			this.notify();
		}
	}
	
	public ByteBuffer getBuffer(){
		return buf;
	}
	
}
