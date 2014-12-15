package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.Map;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seepworker.comm.EventAPI;

public interface OutputAdapter extends API{
	
	public int getStreamId();
	public Map<Integer, OutputBuffer> getOutputBuffers();
	
	public boolean requiresNetwork();
	public boolean requiresFile();
	public void setEventAPI(EventAPI eAPI);
	
}
