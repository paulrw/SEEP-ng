package uk.ac.imperial.lsds.seep.api.state;

import java.util.List;

public interface Mergeable {

	public void merge(List<SeepState> state);
	
}
