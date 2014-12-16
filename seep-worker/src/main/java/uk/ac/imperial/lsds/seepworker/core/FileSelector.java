package uk.ac.imperial.lsds.seepworker.core;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataOrigin;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class FileSelector {

	final private static Logger LOG = LoggerFactory.getLogger(FileSelector.class);
	
	private int numUpstreamResources;
	
	private Reader reader;
	private Thread readerWorker;
	private Writer writer;
	private Thread writerWorker;
	
	public FileSelector(WorkerConfig wc) {
		
	}
	
	public void configureAccept(Map<Integer, DataOrigin> fileOrigins){
		this.numUpstreamResources = fileOrigins.size();
		this.reader = new Reader();
		this.readerWorker = new Thread(this.reader);
		this.readerWorker.setName("File-Reader");
		
		// TODO: Create channels per fileOrigin and let reader to configure them...
	}
	
	public void configureDownstreamFiles(Map<Integer, DataOrigin> fileDest){
		// TODO: implement this, configure writer, etc...
	}
	
	class Reader implements Runnable {

		private boolean working;
		private Selector readSelector;
		
		public Reader(){
			try {
				this.readSelector = Selector.open();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void stop(){
			this.working = false;
			// TODO: more stuff here
		}
		
		@Override
		public void run() {
			LOG.info("Started File Reader worker: {}", Thread.currentThread().getName());
			
		}
		
	}
	
	class Writer implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
}
