package uk.ac.imperial.lsds.seepworker.core;

import java.net.InetAddress;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.PhysicalSeepQuery;
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

	final private Logger LOG = LoggerFactory.getLogger(Conductor.class.getName());
	
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
		this.myIp = myIp;
		this.wc = wc;
		this.dataPort = wc.getInt(WorkerConfig.DATA_PORT);
		int engineType = wc.getInt(WorkerConfig.ENGINE_TYPE);
		engine = ProcessingEngineFactory.buildProcessingEngine(engineType);
		// Use config to get all parameters that configure input, output and engine
		// TODO:
	}
	
	public void startProcessing(){
		LOG.info("Starting processing engine...");
		ns.startNetworkSelector();
		engine.start();
	}
	
	public void stopProcessing(){
		engine.stop();
	}
	
	public void deployPhysicalOperator(PhysicalOperator o, PhysicalSeepQuery query){
		this.task = o.getSeepTask();
		LOG.info("Configuring local task: {}", task.toString());
		// TODO: set up state if any
		
		// This creates one inputAdapter per upstream stream Id
		coreInput = CoreInputFactory.buildCoreInputForOperator(wc, o);
		// This creates one outputAdapter per downstream stream Id
		coreOutput = CoreOutputFactory.buildCoreOutputForOperator(wc, o, query);
		
		this.ns = maybeConfigureNetworkSelector();
		coreOutput.setEventAPI(ns);
		
		engine.setTask(task);
		engine.setSeepState(state);
		engine.setCoreInput(coreInput);
		engine.setCoreOutput(coreOutput);
		
		// Initialize system
		LOG.info("Setting up task...");
		task.setUp(); // setup method of task
		LOG.info("Setting up task...OK");
		if(ns != null) ns.initNetworkSelector(); // start network selector, if any
	}
	
	private NetworkSelector maybeConfigureNetworkSelector(){
		NetworkSelector ns = null;
		if(coreInput.requiresConfiguringNetworkWorker()){
			LOG.info("Configuring networkSelector for input");
			ns = new NetworkSelector(wc, coreInput.getInputAdapterProvider());
			ns.configureAccept(myIp, dataPort);
		}
		if(coreOutput.requiresConfiguringNetworkWorker()){
			LOG.info("Configuring networkSelector for output");
			if(ns == null) ns = new NetworkSelector(wc, coreInput.getInputAdapterProvider());
			Set<OutputBuffer> obufs = coreOutput.getOutputBuffers();
			ns.configureConnect(obufs);
		}
		return ns;
	}
	
	public void plugSeepTask(SeepTask task){
		// TODO: plug and play. this will do stuff with input and output and then delegate the call to engine
		// this pattern should be the default in this conductor controller
	}
	
}
