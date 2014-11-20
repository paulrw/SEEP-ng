package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.Map;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seepworker.comm.EventAPI;

public interface OutputAdapter extends API{

	public final int PER_TUPLE_OVERHEAD_SIZE = 1 + 4 + 4; // control byte + batch_tuples
	
	public int getStreamId();
	public Map<Integer, OutputBuffer> getOutputBuffers();
	
	public boolean requiresNetwork();
	public void setEventAPI(EventAPI eAPI);
	
}
