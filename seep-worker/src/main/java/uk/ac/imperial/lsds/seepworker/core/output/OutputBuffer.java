package uk.ac.imperial.lsds.seepworker.core.output;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
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
		buf = ByteBuffer.allocate(batchSize + TupleInfo.PER_BATCH_OVERHEAD_SIZE);
	}
	
	public int id(){
		return opId;
	}
	
	public Connection getConnection(){
		return c;
	}

	public boolean write(byte[] data) {
		System.out.println("1 remaining: "+buf.remaining()+" < data.length: "+data.length);
		while(buf.remaining() < data.length){
			// If there is not space enough, then block until there is
			waitForSpace();
		}
		synchronized(buf){
			int dataLength = data.length;
			buf.put((byte) 0); // control: 1 byte
			buf.putInt(1); // num_bytes_per_batch: 4 byte FIXME: get this number properly
			System.out.println("position 5: "+(dataLength + TupleInfo.NUM_TUPLES_BATCH_OVERHEAD));
			buf.putInt(dataLength + TupleInfo.NUM_TUPLES_BATCH_OVERHEAD); // batch_size: 4 byte
			System.out.println("DataLength: "+dataLength);
			buf.putInt(dataLength); // tuple_length: 4 byte
			buf.put(data); // data
		}
		// Assumes that all tuples are similar sized...
		System.out.println("2 remaining: "+buf.remaining()+" < data.length: "+data.length);
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
