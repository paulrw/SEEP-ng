package uk.ac.imperial.lsds.seepworker.core;

import java.net.InetAddress;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.SeepState;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.comm.NetworkSelector;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInputFactory;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutputFactory;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;

public class Conductor {

	private WorkerConfig wc;
	
	private int dataPort;
	private InetAddress myIp;
	private NetworkSelector ns;
	
	private CoreInput coreInput;
	private CoreOutput coreOutput;
	private ProcessingEngine engine;
	
	private SeepTask task;
	private SeepState state;
	
	public Conductor(InetAddress myIp, WorkerConfig wc){
		this.wc = wc;
		this.dataPort = wc.getInt(WorkerConfig.DATA_PORT);
		int engineType = wc.getInt(WorkerConfig.ENGINE_TYPE);
		engine = ProcessingEngineFactory.buildProcessingEngine(engineType);
		// Use config to get all parameters that configure input, output and engine
		// TODO:
	}
	
	public void start(){
		engine.start();
	}
	
	public void stop(){
		engine.stop();
	}
	
	public void deployPhysicalOperator(PhysicalOperator o){
		// This creates one inputAdapter per upstream stream Id
		coreInput = CoreInputFactory.buildCoreInputForOperator(wc, o);
		// This creates one outputAdapter per downstream stream Id
		coreOutput = CoreOutputFactory.buildCoreOutputForOperator(o);
		
		this.ns = maybeConfigureNetworkSelector();
		coreOutput.setEventAPI(ns);
		
		engine.setTask(task);
		engine.setSeepState(state);
		engine.setCoreInput(coreInput);
		engine.setCoreOutput(coreOutput);
		
		// Initialize system
		task.setUp(); // setup method of task
		engine.start(); // start engine processing loop
		if(ns != null) ns.start(); // start network selector, if any
	}
	
	private NetworkSelector maybeConfigureNetworkSelector(){
		NetworkSelector ns = null;
		if(coreInput.requiresConfiguringNetworkWorker()){
			ns = new NetworkSelector(wc, coreInput.getInputAdapterProvider());
			ns.configureAccept(myIp, dataPort);
		}
		if(coreOutput.requiresConfiguringNetworkWorker()){
			if(ns == null) ns = new NetworkSelector(wc, coreInput.getInputAdapterProvider());
			Set<OutputBuffer> obufs = coreOutput.getOutputBuffers();
			ns.configureConnect(obufs);
		}
		return ns;
	}
	
	public void startProcessing(){
		// TODO: figure out whether it's necessary to differentiate sources from the rest or not...
	}
	
	public void plugSeepTask(SeepTask task){
		// TODO: plug and play. this will do stuff with input and output and then delegate the call to engine
		// this pattern should be the default in this conductor controller
	}
	
}
