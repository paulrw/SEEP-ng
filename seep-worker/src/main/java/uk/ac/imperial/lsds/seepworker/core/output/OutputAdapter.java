package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.Map;

import uk.ac.imperial.lsds.seep.api.API;

public interface OutputAdapter extends API{

	public int getStreamId();
	public Map<Integer, OutputBuffer> getOutputBuffers();
	
}
