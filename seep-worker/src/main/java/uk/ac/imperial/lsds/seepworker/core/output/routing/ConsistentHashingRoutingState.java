package uk.ac.imperial.lsds.seepworker.core.output.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import uk.ac.imperial.lsds.seep.api.DownstreamConnection;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer2;


public class ConsistentHashingRoutingState implements RoutingState {

	private CRC32 crc32;
	
	private List<DownstreamConnection> cons;
	
	private List<Integer> ids;
	private List<Integer> subspaceFrontiers;
	
	public ConsistentHashingRoutingState(List<DownstreamConnection> cons){
		this.crc32 = new CRC32();
		this.cons = cons;
		this.ids = new ArrayList<>();
		this.subspaceFrontiers = new ArrayList<>();
		// split initial space into the number of cons
		int numSplits = cons.size() - 1;
		// calculate span of each subrange of the space
		long entireSpace = Integer.MAX_VALUE * 2;
		
		long initialSpaceRange = entireSpace/numSplits;

		int horizon = Integer.MIN_VALUE;
		for(int i = 0; i < cons.size(); i++){
			// get id to which we'll assign this subspace
			int id = cons.get(i).getDownstreamOperator().getOperatorId();
			int frontier = horizon + (int)initialSpaceRange;
			ids.add(id);
			subspaceFrontiers.add(frontier);
		}
	}

	@Override
	public OutputBuffer route(Map<Integer, OutputBuffer> obufs, int key) {
		int hashedKey = hashKey(key);
		for(int i = 0; i<subspaceFrontiers.size(); i++){
			int frontier = subspaceFrontiers.get(i);
			if(hashedKey < frontier){
				int id = ids.get(i);
				return obufs.get(id);
			}
		}
		return null;
	}
	
	private int hashKey(int value){
		crc32.update(value);
		int v = (int)crc32.getValue();
		crc32.reset();
		return v;
	}
	
	private int hashKey(String value){
		int v = value.hashCode();
		return hashKey(v);
	}
	
}
