package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepworker.comm.EventAPI;

public class CoreOutput {
	
	final private static Logger LOG = LoggerFactory.getLogger(CoreOutput.class);
	
	private List<OutputAdapter> outputAdapters;
		
	public CoreOutput(List<OutputAdapter> outputAdapters){
		this.outputAdapters = outputAdapters;
		LOG.info("Configured CoreOutput with {} outputAdapters", outputAdapters.size());
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
	
	public boolean requiresConfiguringFileWorker(){
		for(OutputAdapter oa : outputAdapters){
			if(oa.requiresFile())
				return true;
		}
		return false;
	}
	
	public void setEventAPI(EventAPI eAPI){
		for(OutputAdapter oa : outputAdapters){
			oa.setEventAPI(eAPI);
		}
	}
	
}
