package uk.ac.imperial.lsds.seepworker.core.output;

import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.DownstreamConnection;
import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;

public class OutputAdapterFactory {

	public static OutputAdapter buildOutputAdapterOfTypeNetworkForOps(int streamId, List<DownstreamConnection> cons, Selector s){
		// Create a router for the outputAdapter with the downstreamConn info
		Router r = Router.buildRouterFor(cons);

		// Get a map of id-outputBuffer, where id is the downstream op id
		Map<Integer, OutputBuffer> outputBuffers = new HashMap<>();
		for(DownstreamConnection dc : cons){
			int id = dc.getDownstreamOperator().getOperatorId();
			Connection c = new Connection(((PhysicalOperator)dc.getDownstreamOperator()).getWrappingEndPoint());
			OutputBuffer ob = new OutputBuffer(id, c);
			outputBuffers.put(id, ob);
		}
		// TODO: left for configuration whether this should be a simpleoutput or something else...
		OutputAdapter oa = new SimpleOutput(streamId, r, outputBuffers, s);
		return oa;
	}

}
