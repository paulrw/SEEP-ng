package uk.ac.imperial.lsds.seepworker.core.output;

import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.DataOrigin;
import uk.ac.imperial.lsds.seep.api.DownstreamConnection;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.comm.NetworkWriter;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;

public class OutputAdapterFactory {

	public static OutputAdapter buildOutputAdapterOfTypeForOps(int streamId, List<DownstreamConnection> cons){
		
		DataOrigin dOrigin = cons.get(0).getExpectedDataOriginOfDownstream();
		Map<Integer, OutputBuffer> outputBuffers = createOutputQueuesFor(cons);
		Router r = Router.buildRouterFor(cons);
		
		//List<Connection> connections = getConnectionsFrom(cons);
		
		if(dOrigin == DataOrigin.NETWORK) {
			//NetworkWriter nw = new NetworkWriter(cons, outputBuffers);
		}
		
		OutputAdapter oa = new SimpleOutput(streamId, r, outputBuffers, dOrigin);
		return oa;
	}
	
	private static Map<Integer, OutputBuffer> createOutputQueuesFor(List<DownstreamConnection> cons){
		Map<Integer, OutputBuffer> outputs = new HashMap<>();
		for(DownstreamConnection dc : cons){
			OutputBuffer ob = new OutputBuffer(dc.getStreamId(), dc.getDownstreamOperator().getOperatorId());
			outputs.put(dc.getDownstreamOperator().getOperatorId(), ob);
		}
		return outputs;
	}
}
