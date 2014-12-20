package uk.ac.imperial.lsds.seepworker.core.output.routing;

import java.util.Map;

import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;

public interface RoutingState {

	public OutputBuffer route(Map<Integer, OutputBuffer> obufs, int key);
	
}
