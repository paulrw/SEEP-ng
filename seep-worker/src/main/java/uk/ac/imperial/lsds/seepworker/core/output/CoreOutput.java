package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import uk.ac.imperial.lsds.seep.comm.Connection;


public class CoreOutput {
	
	private List<OutputAdapter> outputAdapters;
		
	public CoreOutput(List<OutputAdapter> outputAdapters){
		this.outputAdapters = outputAdapters;
	}
	
	public List<OutputAdapter> getOutputAdapters(){
		return outputAdapters;
	}
	
	public Set<OutputBuffer> getOutputBuffers(){
		Set<OutputBuffer> cons = new HashSet<>();
		for(OutputAdapter oa : outputAdapters){
			cons.addAll(oa.getOutputBuffers().values());
		}
		return cons;
	}
	
	public boolean requiresConfiguringNetworkWorker(){
		for(OutputAdapter oa : outputAdapters){
			if(oa.requiresNetwork())
				return true;
		}
		return false;
	}
	
}
