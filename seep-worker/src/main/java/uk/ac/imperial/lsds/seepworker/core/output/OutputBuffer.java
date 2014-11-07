package uk.ac.imperial.lsds.seepworker.core.output;

import java.nio.ByteBuffer;

public class OutputBuffer {

	private int streamId;
	private int id;
	
	private ByteBuffer buf;
	
	public OutputBuffer(int streamId, int operatorId) {
		this.streamId = streamId;
		this.id = operatorId;
	}

	public void write(byte[] data){
		// control batching number and size, etc...
		buf.put(data);
	}
	
	public ByteBuffer getBuffer(){
		return buf;
	}
	
}
