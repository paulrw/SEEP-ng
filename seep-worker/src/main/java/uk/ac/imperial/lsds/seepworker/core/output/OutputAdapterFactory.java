package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.DownstreamConnection;
import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.PhysicalSeepQuery;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;

public class OutputAdapterFactory {

	public static OutputAdapter2 buildOutputAdapterOfTypeNetworkForOps(WorkerConfig wc, int streamId, 
			List<DownstreamConnection> cons, PhysicalSeepQuery query){
		// Create a router for the outputAdapter with the downstreamConn info
		Router r = Router.buildRouterFor(cons);

		// Get a map of id-outputBuffer, where id is the downstream op id
		Map<Integer, OutputBuffer2> outputBuffers = new HashMap<>();
		for(DownstreamConnection dc : cons){
			int id = dc.getDownstreamOperator().getOperatorId();
			PhysicalOperator downstreamPhysOperator = query.getOperatorWithId(dc.getDownstreamOperator().getOperatorId());
			Connection c = new Connection(downstreamPhysOperator.getWrappingEndPoint());
			OutputBuffer2 ob = new OutputBuffer2(wc, id, c, streamId);
			outputBuffers.put(id, ob);
		}
		// TODO: left for configuration whether this should be a simpleoutput or something else...
		OutputAdapter2 oa = new SimpleNetworkOutput2(streamId, r, outputBuffers);
		return oa;
	}

}
