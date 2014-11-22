package uk.ac.imperial.lsds.seepworker.core.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;

public class InputBuffer {
	
	private ByteBuffer buffer;
	
	public List<byte[]> completedReads;
	
	public InputBuffer(int size){
		buffer = ByteBuffer.allocate(size*4);
		completedReads = new ArrayList<>();
	}
	
	public boolean readFrom(Channel channel){
		try {
			int readBytes = ((SocketChannel)channel).read(buffer);
			System.out.println("just read: "+readBytes);
			int position = buffer.position(); // tells us how much have been read, this may be called several times
			if(position < TupleInfo.PER_BATCH_OVERHEAD_SIZE){
				return false; // we do not have enough info for even one tuple
			}
			// fresh read
			System.out.println("FRESH READ, current position: "+buffer.position());
			buffer.position(TupleInfo.CONTROL_OVERHEAD);
			int numTuples = buffer.getInt();
			int batchSize = buffer.getInt();
			int remaining = buffer.remaining();
			System.out.println("numTuples: "+numTuples+" remaining: "+remaining+" batchSize: "+batchSize);
			if(remaining >= batchSize){ // >= cause we do not have exact sized batches due to variable sized tuples
//				System.out.println("a");
				for(int i = 0; i < numTuples; i++){
//					System.out.println("b");
					int tupleSize = buffer.getInt();
					System.out.println("Tuple size: "+tupleSize);
					byte[] completedRead = new byte[tupleSize];
					System.out.println("Read from: "+buffer.position()+" these many bytes: "+tupleSize);
					buffer.get(completedRead, 0, tupleSize);
					System.out.println("completedRead length: "+completedRead.length);
					completedReads.add(completedRead);
				}
				buffer.clear(); // leave buffer ready for next read op
				return true;
			}
			else{
				return false;
			}
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
}
