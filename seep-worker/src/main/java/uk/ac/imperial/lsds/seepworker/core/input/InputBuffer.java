package uk.ac.imperial.lsds.seepworker.core.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;

public class InputBuffer {
	
	private ByteBuffer buffer;
	
	public Deque<byte[]> completedReads;
	
	public InputBuffer(int size){
		buffer = ByteBuffer.allocate(size);
		completedReads = new ArrayDeque<>();
	}
	
	public boolean canReadFullBatch(int fromPosition, int limit){
		// Check whether we can read a complete batch

		int initialPosition = buffer.position();
		int initialLimit = buffer.limit();
		
		buffer.position(fromPosition);
		buffer.limit(limit);
		int remaining = buffer.remaining();
		if(remaining < TupleInfo.PER_BATCH_OVERHEAD_SIZE){
			// Reset buffer back to initial status and wait for more data to arrive
			buffer.limit(initialLimit);
			buffer.position(initialPosition);
			return false;
		} 
		else{
			buffer.position(fromPosition + TupleInfo.BATCH_SIZE_OFFSET);
			int batchSize = buffer.getInt();
			buffer.limit(initialLimit);
			buffer.position(initialPosition);
			if(remaining < batchSize){
				return false;
			}
			return true;
		}
	}
	
	public boolean readFrom(Channel channel, InputAdapter ia){
		boolean dataRemainingInBuffer = true;
		int readBytes = 0;
		try {
			readBytes = ((SocketChannel)channel).read(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int initialLimit = buffer.position();
		int fromPosition = 0;
		while(dataRemainingInBuffer){
			if(canReadFullBatch(fromPosition, initialLimit)){
				buffer.limit(initialLimit);
				buffer.position(fromPosition);
				
				byte control = buffer.get();
				int numTuples = buffer.getInt();
				int batchSize = buffer.getInt();
				for(int i = 0; i < numTuples; i++){
					int tupleSize = buffer.getInt();
					byte[] completedRead = new byte[tupleSize];
					buffer.get(completedRead, 0, tupleSize);
					ia.pushData(completedRead);
				}
				fromPosition = buffer.position(); // Update position for next iteration
			}
			else{
				if(buffer.hasRemaining()){
					buffer.compact(); // make space to complete chunked read
					return false;
				}
				else{
					dataRemainingInBuffer = false;
					buffer.clear();
					return true; // Fully read buffer
				}
			}
		}
		return false;
	}
}
