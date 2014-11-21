package uk.ac.imperial.lsds.seepworker.core.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;

public class InputBuffer {
	
//	private int expectedSize = -1;
	private ByteBuffer buffer;
	
	public List<byte[]> completedReads;
	
	public InputBuffer(int size){
		buffer = ByteBuffer.allocate(size);
		completedReads = new ArrayList<>();
	}
	
	public boolean readFrom(Channel channel){
		System.out.println("Here we are...");
		try {
			int readBytes = ((SocketChannel)channel).read(buffer);
			System.out.println("just read: "+readBytes);
			int position = buffer.position();
			System.out.println("Position after reading: "+position+" compare to: "+TupleInfo.PER_BATCH_OVERHEAD_SIZE);
			if(position < TupleInfo.PER_BATCH_OVERHEAD_SIZE){
				return false;
			}
//			if(expectedSize != -1){
//				if(position >= expectedSize){
//					// new read completed here
//					byte[] completedRead = new byte[expectedSize];
//					buffer.get(completedRead, TupleInfo.BATCH_SIZE_OFFSET, position);
//					completedReads.add(completedRead);
//					expectedSize = -1;
//					buffer.compact();
//					return true;
//				}
//				else{
//					return false;
//				}
//			}
//			else{
			// fresh read
			System.out.println("FRESH READ, current position: "+buffer.position());
			buffer.position(TupleInfo.CONTROL_OVERHEAD);
			int numTuples = buffer.getInt();
			int batchSize = buffer.getInt();
//			expectedSize = batchSize;
			int remaining = buffer.remaining();
			System.out.println("numTuples: "+numTuples+" remaining: "+remaining+" batchSize: "+batchSize);
			if(remaining >= batchSize){
				for(int i = 0; i < numTuples; i++){
					int tupleSize = buffer.getInt();
					System.out.println("Tuple size: "+tupleSize);
					byte[] completedRead = new byte[tupleSize];
					System.out.println("Read from: "+buffer.position()+" these many bytes: "+tupleSize);
					buffer.get(completedRead, 0, tupleSize);
					System.out.println("completedRead length: "+completedRead.length);
					completedReads.add(completedRead);
				}
//				expectedSize = -1;
				buffer.compact();
				return true;
			}
			else{
				return false;
			}
//			}
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
}
