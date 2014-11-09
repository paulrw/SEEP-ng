package uk.ac.imperial.lsds.seepworker.core.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class InputBuffer {
	
	private ByteBuffer buf;
	
	public InputBuffer(){
		
	}
	
	public void networkRead(SocketChannel channel){
		try {
			channel.read(buf);
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
