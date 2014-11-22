package uk.ac.imperial.lsds.seepworker.core.output;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class OutputBuffer {

	private int opId;
	private Connection c;
	
	private ByteBuffer buf;
	private int batchSize;
	private Deque<byte[]> batchQueue;
	private int currentPayloadSize = 0;
	
	public OutputBuffer(WorkerConfig wc, int opId, Connection c){
		this.opId = opId;
		this.c = c;
		this.batchSize = wc.getInt(WorkerConfig.BATCH_SIZE);
		buf = ByteBuffer.allocate(batchSize + TupleInfo.PER_BATCH_OVERHEAD_SIZE);
		batchQueue = new ArrayDeque<>();
	}
	
	public int id(){
		return opId;
	}
	
	public Connection getConnection(){
		return c;
	}

	public boolean write(byte[] data) {
		// If there is no data in the queue, then we can always add, regardless any other parameter
		if(currentPayloadSize == 0){
			batchQueue.add(data);
			currentPayloadSize = data.length + TupleInfo.TUPLE_SIZE_OVERHEAD;
		}
		int sizeIfDataIsWritten = TupleInfo.PER_BATCH_OVERHEAD_SIZE
				+ currentPayloadSize 
				+ data.length 
				+ TupleInfo.TUPLE_SIZE_OVERHEAD;
		System.out.println("sizeIfDataIsWritten: "+sizeIfDataIsWritten+" batchSize: "+batchSize);
		if(sizeIfDataIsWritten > batchSize){
			if(buf.remaining() < currentPayloadSize){
				// In this case, we have a batch completed, but the queue is full, so we should block
				waitForSpace();
			}
			System.out.println("buffer position should be zero: "+buf.position());
			// We can and must send the batch now
			int numTuplesInBatch = batchQueue.size();
			buf.put((byte)0); // control: 1 byte
			buf.putInt(numTuplesInBatch); // num_tuples: 4 bytes
			buf.putInt(currentPayloadSize); // batch_size: 4 bytes
			for(int i = 0; i < numTuplesInBatch; i++){
				byte[] el = batchQueue.poll();
				buf.putInt(el.length);
				buf.put(el);
			}
			System.out.println("Buf position should be whatever is written: "+buf.position());
			// add first tuple of the next batch
			batchQueue.add(data);
			// reset the size
			currentPayloadSize = data.length + TupleInfo.TUPLE_SIZE_OVERHEAD;
			System.out.println("currentPayloadSize after writing: "+currentPayloadSize);
			return true;
		}
		else{
			// queue, cannot send yet
			batchQueue.add(data);
			currentPayloadSize = currentPayloadSize + data.length + TupleInfo.TUPLE_SIZE_OVERHEAD;
			return false;
		}
	}
	
	private void waitForSpace(){
		try {
			synchronized(this){
				this.wait();
			}
		}
		catch (InterruptedException e) {
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
