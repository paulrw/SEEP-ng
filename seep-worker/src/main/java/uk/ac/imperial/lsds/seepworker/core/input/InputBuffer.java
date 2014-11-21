package uk.ac.imperial.lsds.seepworker.core.input;

import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.ArrayList;
import java.util.List;

public class InputBuffer {
	
	private ByteBuffer buf;
	
	public List<byte[]> completedReads;
	
	public InputBuffer(int size){
		buf = ByteBuffer.allocate(size);
		completedReads = new ArrayList<>();
	}
	
	public boolean readFrom(Channel channel){
		
		return false;
	}
}
