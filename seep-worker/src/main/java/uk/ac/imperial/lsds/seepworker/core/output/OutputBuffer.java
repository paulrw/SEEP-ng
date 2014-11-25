package uk.ac.imperial.lsds.seepworker.core.output;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class OutputBuffer {
	
	private int opId;
	private Connection c;
	private int streamId;
	
	private ByteBuffer buf;
	private int batchSize;
	public BatchQueue bq;
	
	public OutputBuffer(WorkerConfig wc, int opId, Connection c, int streamId){
		this.opId = opId;
		this.c = c;
		this.streamId = streamId;
		this.batchSize = wc.getInt(WorkerConfig.BATCH_SIZE);
		buf = ByteBuffer.allocate(wc.getInt(WorkerConfig.SEND_APP_BUFFER_SIZE));
		bq = new BatchQueue();
	}
	
	public int getStreamId(){
		return streamId;
	}
	
	public int id(){
		return opId;
	}
	
	public Connection getConnection(){
		return c;
	}
	
	public boolean write(byte[] data){
		// Try to add data, will block if space bigger than batch
		boolean canSend = bq.add(data);
		// by definition will only return when data can actually be written
		return canSend;
	}
	
	public ByteBuffer getBuffer(){
		return buf;
	}
	
	public class BatchQueue{
		
		private List<byte[]> ongoingBatch;
		private BlockingQueue<List<byte[]>> bq;
		private int currentPayloadSize = 0;

		public BatchQueue(){
			this.ongoingBatch = new ArrayList<>();
			this.bq = new ArrayBlockingQueue<>(1);
		}
		
		public boolean add(byte[] data){
			int sizeOfBatchWithCurrentElement = currentPayloadSize + data.length + TupleInfo.TUPLE_SIZE_OVERHEAD;
			if(sizeOfBatchWithCurrentElement > batchSize){
				if(ongoingBatch.size() == 0){
					ongoingBatch.add(data);
					currentPayloadSize = currentPayloadSize + data.length + TupleInfo.TUPLE_SIZE_OVERHEAD;
				}
				try {
					ArrayList<byte []> copyToSend = new ArrayList<>(ongoingBatch);
					bq.put(copyToSend);
					ongoingBatch.clear();
					currentPayloadSize = 0;
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false; // with error say batch is not complete
				}
				return true;
			}
			else{
				ongoingBatch.add(data);
				currentPayloadSize = currentPayloadSize + data.length + TupleInfo.TUPLE_SIZE_OVERHEAD;
				return false;
			}
		}
		
		public List<byte []> poll(){
			List<byte[]> datas = bq.poll();
			return datas;
		}	
	}
}