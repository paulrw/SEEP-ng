package uk.ac.imperial.lsds.seep.api.state;

import java.util.List;

public interface Partitionable {

	public List<? extends SeepState> partition();
	public List<? extends SeepState> partition(int partitions);
	
}
