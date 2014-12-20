package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.api.state.SeepState;

public interface LogicalState {

	public int getStateId();
	public SeepState getStateElement();
	
}
