package uk.ac.imperial.lsds.seepworker.core.output;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.seep.comm.Connection;

public class OutputBuffer {

	private int opId;
	private Connection c;
	
	private ByteBuffer buf;
	
	public OutputBuffer(int opId, Connection c){
		this.opId = opId;
		this.c = c;
	}
	
	public int id(){
		return opId;
	}
	
	public Connection getConnection(){
		return c;
	}

	public int write(byte[] data){
		// control batching number and size, etc...
		buf.put(data);
		return buf.remaining();
	}
	
	public ByteBuffer getBuffer(){
		return buf;
	}
	
}
