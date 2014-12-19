package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.Operator;
import uk.ac.imperial.lsds.seep.api.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class InputAdapterFactory {

	final static private Logger LOG = LoggerFactory.getLogger(IOComm.class.getName());
	
	public static InputAdapter buildInputAdapterOfTypeNetworkForOps(WorkerConfig wc, int streamId, List<UpstreamConnection> upc){
		InputAdapter ia = null;
		short cType = upc.get(0).getConnectionType().ofType();
		Schema expectedSchema = upc.get(0).getExpectedSchema();
		
		if(cType == ConnectionType.ONE_AT_A_TIME.ofType()){
			// one-queue-per-conn, one-single-queue, etc.
			LOG.info("Creating inputAdapter for upstream streamId: {} of type {}", streamId, "ONE_AT_A_TIME");
			List<Operator> ops = new ArrayList<>();
			for(UpstreamConnection uc : upc){
				ops.add(uc.getUpstreamOperator());
			}
			ia = new NetworkDataStream(wc, streamId, expectedSchema, ops);
		}
		else if(cType == ConnectionType.UPSTREAM_SYNC_BARRIER.ofType()){
			// one barrier for all connections within the same barrier
			LOG.info("Creating NETWORK inputAdapter for upstream streamId: {} of type {}", streamId, ConnectionType.UPSTREAM_SYNC_BARRIER.withName());
			ia = new NetworkBarrier(wc, streamId, expectedSchema, upc);
		}
		else if(cType == ConnectionType.BATCH.ofType()){
			
		}
		else if(cType == ConnectionType.ORDERED.ofType()){
			
		}
		else if(cType == ConnectionType.WINDOW.ofType()){
			
		}
		return ia;
	}
	
	public static InputAdapter buildInputAdapterOfTypeFileForOps(WorkerConfig wc, int streamId, List<UpstreamConnection> upc){
		InputAdapter ia = null;
		short cType = upc.get(0).getConnectionType().ofType();
		Schema expectedSchema = upc.get(0).getExpectedSchema();
		
		if(cType == ConnectionType.ONE_AT_A_TIME.ofType()){
			LOG.info("Creating FILE inputAdapter for upstream streamId: {} of type {}", streamId, ConnectionType.ONE_AT_A_TIME.withName());
			ia = new FileDataStream(wc, streamId, expectedSchema, upc);
		}
		else if(cType == ConnectionType.BATCH.ofType()){
			
		}
		
		return ia;
	}
	
}
