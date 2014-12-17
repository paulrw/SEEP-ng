package uk.ac.imperial.lsds.seepworker.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.Selector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataOrigin;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.InputAdapter;

public class FileSelector {

	final private static Logger LOG = LoggerFactory.getLogger(FileSelector.class);
	
	private int numUpstreamResources;
	
	private Reader reader;
	private Thread readerWorker;
	private Writer writer;
	private Thread writerWorker;
	
	private Map<Integer, InputAdapter> dataAdapters;
	
	public FileSelector(WorkerConfig wc) {
		
	}
	
	public void configureAccept(Map<Integer, DataOrigin> fileOrigins, Map<Integer, InputAdapter> dataAdapters){
		this.dataAdapters = dataAdapters;
		this.numUpstreamResources = fileOrigins.size();
		this.reader = new Reader();
		this.readerWorker = new Thread(this.reader);
		this.readerWorker.setName("File-Reader");
		
		Map<SeekableByteChannel, Integer> channels = new HashMap<>();
		for(Entry<Integer, DataOrigin> e : fileOrigins.entrySet()){
			try {
				Path resource = Paths.get(new URI(e.getValue().getResourceDescriptor()));
				SeekableByteChannel sbc = Files.newByteChannel(resource, StandardOpenOption.READ);
				channels.put(sbc, e.getKey());
			} 
			catch (FileNotFoundException fnfe) {
				fnfe.printStackTrace();
			} catch (URISyntaxException use) {
				use.printStackTrace();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		this.reader.availableChannels(channels);
	}
	
	public void configureDownstreamFiles(Map<Integer, DataOrigin> fileDest){
		// TODO: implement this, configure writer, etc...
	}
	
	class Reader implements Runnable {

		private boolean working;
		private Selector readSelector;
		private Map<SeekableByteChannel, Integer> channels;
		
		public Reader(){
			try {
				this.readSelector = Selector.open();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void availableChannels(Map<SeekableByteChannel, Integer> channels){
			this.channels = channels;
		}
		
		public void stop(){
			this.working = false;
			// TODO: more stuff here
		}
		
		@Override
		public void run() {
			LOG.info("Started File Reader worker: {}", Thread.currentThread().getName());
			
			for(Entry<SeekableByteChannel, Integer> e: channels.entrySet()){
				int id = e.getValue();
				ReadableByteChannel rbc = e.getKey();
				InputAdapter ia = dataAdapters.get(id);
				if(rbc.isOpen()){
					ia.readFrom(rbc, id);
				}
			}
			
		}
		
	}
	
	class Writer implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
}
