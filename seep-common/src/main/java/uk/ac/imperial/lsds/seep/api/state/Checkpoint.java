package uk.ac.imperial.lsds.seep.api.state;

public interface Checkpoint {

	public byte[] checkpoint();
	public void recover(byte[] bytes);
	
}
