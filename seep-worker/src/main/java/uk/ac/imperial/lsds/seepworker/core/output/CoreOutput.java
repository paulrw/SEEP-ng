package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepworker.comm.EventAPI;

public class CoreOutput {
	
	final private static Logger LOG = LoggerFactory.getLogger(CoreOutput.class);
	
	private List<OutputAdapter2> outputAdapters;
		
	public CoreOutput(List<OutputAdapter2> outputAdapters){
		this.outputAdapters = outputAdapters;
		LOG.info("Configured CoreOutput with {} outputAdapters", outputAdapters.size());
	}
	
	public List<OutputAdapter2> getOutputAdapters(){
		return outputAdapters;
	}
	
	public Set<OutputBuffer2> getOutputBuffers(){
		Set<OutputBuffer2> cons = new HashSet<>();
		for(OutputAdapter2 oa : outputAdapters){
			cons.addAll(oa.getOutputBuffers().values());
		}
		return cons;
	}
	
	public boolean requiresConfiguringNetworkWorker(){
		for(OutputAdapter2 oa : outputAdapters){
			if(oa.requiresNetwork())
				return true;
		}
		return false;
	}
	
	public boolean requiresConfiguringFileWorker(){
		for(OutputAdapter2 oa : outputAdapters){
			if(oa.requiresFile())
				return true;
		}
		return false;
	}
	
	public void setEventAPI(EventAPI eAPI){
		for(OutputAdapter2 oa : outputAdapters){
			oa.setEventAPI(eAPI);
		}
	}
	
}
