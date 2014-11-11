package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.UpstreamConnection;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class CoreInputFactory {

	public static CoreInput buildCoreInputForOperator(WorkerConfig wc, PhysicalOperator o){
		List<InputAdapter> inputAdapters = new LinkedList<>();
		// Create an InputAdapter per upstream connection -> know with the streamId
		Map<Integer, List<UpstreamConnection>> streamToOpConn = new HashMap<>();
		for(UpstreamConnection uc : o.upstreamConnections()){
			int streamId = uc.getStreamId();
			if(streamToOpConn.containsKey(streamId)){
				streamToOpConn.get(streamId).add(uc);
			}
			else{
				List<UpstreamConnection> l = new ArrayList<>();
				l.add(uc);
				streamToOpConn.put(streamId, l);
			}
		}
		// Perform sanity check. All ops for a given streamId should have the same connType
		for(List<UpstreamConnection> l : streamToOpConn.values()){
			ConnectionType ct = null;
			for(UpstreamConnection oct : l){
				if(ct == null){
					ct = oct.getConnectionType();
				}
				if(!ct.equals(oct.getConnectionType())){
					// TODO: throw error
					System.out.println("Sanity check FAILED");
					System.exit(0);
				}
			}
		}
		// Build an input adapter per streamId
		for(Integer streamId : streamToOpConn.keySet()){
			List<UpstreamConnection> upCon = streamToOpConn.get(streamId);
			InputAdapter ia = InputAdapterFactory.buildInputAdapterOfTypeForOps(wc, streamId, upCon);
			inputAdapters.add(ia);
		}
		CoreInput cInput = new CoreInput(inputAdapters);
		return cInput;
	}
	
}
