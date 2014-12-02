package uk.ac.imperial.lsds.seepworker.comm;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.InputAdapter;
import uk.ac.imperial.lsds.seepworker.core.input.InputBuffer;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;

public class NetworkSelector implements EventAPI {

	final private static Logger LOG = LoggerFactory.getLogger(NetworkSelector.class);
	
	private ServerSocketChannel listenerSocket;
	private Selector acceptorSelector;
	private boolean acceptorWorking = false;
	private Thread acceptorWorker;
	
	private Reader[] readers;
	private Writer[] writers;
	private int numReaderWorkers;
	private int totalNumberPendingConnectionsPerThread;
	
	private Thread[] readerWorkers;
	private Thread[] writerWorkers;
	private int numWriterWorkers;
	
	private Map<Integer, SelectionKey> writerKeys;
	
	// incoming id - local input adapter
	private Map<Integer, InputAdapter> iapMap;
	private int numUpstreamConnections;
	
	public NetworkSelector(WorkerConfig wc, Map<Integer, InputAdapter> iapMap) {
		this.iapMap = iapMap;
		this.numUpstreamConnections  = iapMap.size();
		LOG.info("Expecting {} upstream connections", numUpstreamConnections);
		this.numReaderWorkers = wc.getInt(WorkerConfig.NUM_NETWORK_READER_THREADS);
		this.numWriterWorkers = wc.getInt(WorkerConfig.NUM_NETWORK_WRITER_THREADS);
		this.totalNumberPendingConnectionsPerThread = wc.getInt(WorkerConfig.MAX_PENDING_NETWORK_CONNECTION_PER_THREAD);
		LOG.info("Configuring NetworkSelector with: {} readers, {} workers and {} maxPendingNetworkConn",
				numReaderWorkers, numWriterWorkers, totalNumberPendingConnectionsPerThread);
		// Create pool of reader threads
		readers = new Reader[numReaderWorkers];
		readerWorkers = new Thread[numReaderWorkers];
		for(int i = 0; i < numReaderWorkers; i++){
			readers[i] = new Reader(i, totalNumberPendingConnectionsPerThread);
			Thread reader = new Thread(readers[i]);
			reader.setName("Network-Reader-"+i);
			readerWorkers[i] = reader;
		}
		// Create pool of writer threads
		writers = new Writer[numWriterWorkers];
		writerWorkers = new Thread[numWriterWorkers];
		for(int i = 0; i < numWriterWorkers; i++){
			writers[i] = new Writer(i);
			Thread writer = new Thread(writers[i]);
			writer.setName("Network-Writer-"+i);
			writerWorkers[i] = writer;
		}
		this.writerKeys = new HashMap<>();
		// Create the acceptorSelector
		try {
			this.acceptorSelector = Selector.open();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// TODO: to avoid confusion make this a method called from tests, rather than a different constructor
	public NetworkSelector(Map<Integer, InputAdapter> iapMap){
		this.iapMap = iapMap;
		this.numUpstreamConnections  = iapMap.size();
		LOG.info("Expecting {} upstream connections", numUpstreamConnections);
		this.numReaderWorkers = 1;
		this.numWriterWorkers = 1;
		this.totalNumberPendingConnectionsPerThread = 1;
		LOG.info("Configuring NetworkSelector with: {} readers, {} workers and {} maxPendingNetworkConn",
				numReaderWorkers, numWriterWorkers, totalNumberPendingConnectionsPerThread);
		// Create pool of reader threads
		readers = new Reader[numReaderWorkers];
		readerWorkers = new Thread[numReaderWorkers];
		for(int i = 0; i < numReaderWorkers; i++){
			readers[i] = new Reader(i, totalNumberPendingConnectionsPerThread);
			readerWorkers[i] = new Thread(readers[i]);
		}
		// Create pool of writer threads
		writers = new Writer[numWriterWorkers];
		writerWorkers = new Thread[numWriterWorkers];
		for(int i = 0; i < numWriterWorkers; i++){
			writers[i] = new Writer(i);
			writerWorkers[i] = new Thread(writers[i]);
		}
		this.writerKeys = new HashMap<>();
		// Create the acceptorSelector
		try {
			this.acceptorSelector = Selector.open();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void readyForWrite(int id){
		SelectionKey key = writerKeys.get(id);
		synchronized(key){
			int interestOps = key.interestOps() | SelectionKey.OP_WRITE;
			key.interestOps(interestOps);
			key.selector().wakeup();
		}
	}
	
	@Override
	public void readyForWrite(List<Integer> ids){
		for(Integer id : ids){
			SelectionKey key = writerKeys.get(id);
			synchronized(key){
				int interestOps = key.interestOps() | SelectionKey.OP_WRITE;
				key.interestOps(interestOps);
				key.selector().wakeup();
			}
		}
	}

	public void configureAccept(InetAddress myIp, int dataPort){
		ServerSocketChannel channel = null;
		try {
			channel = ServerSocketChannel.open();
			SocketAddress sa = new InetSocketAddress(myIp, dataPort);
			channel.configureBlocking(false);
			channel.bind(sa);
			channel.register(acceptorSelector, SelectionKey.OP_ACCEPT);
			LOG.info("Configured Acceptor thread to listen at: {}", sa.toString());
		}
		catch (ClosedChannelException cce) {
			// TODO Auto-generated catch block
			cce.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.listenerSocket = channel;
		this.acceptorWorker = new Thread(new AcceptorWorker());
		this.acceptorWorker.setName("Network-Acceptor");
	}
	
	public void configureConnect(Set<OutputBuffer> obufs){
		int writerIdx = 0;
		int totalWriters = writers.length;
		for(OutputBuffer obuf : obufs){
			writers[(writerIdx++)%totalWriters].newConnection(obuf);
		}
	}
	
	public void start(){
		LOG.info("Starting network selector thread...");
		this.acceptorWorking = true;
		// Check whether there is a network acceptor worker. There won't be one if there are no input network connections.
		if(acceptorWorker != null){
			LOG.info("Starting acceptor thread: {}", acceptorWorker.getName());
			this.acceptorWorker.start();
		}
		// Start readers
		for(Thread r : readerWorkers){
			LOG.info("Starting reader: {}", r.getName());
			r.start();
		}
		// Start writers
		for(Thread w : writerWorkers){
			LOG.info("Starting writer: {}", w.getName());
			w.start();
		}
		LOG.info("Starting network selector thread...OK");
	}
	
	public void stop(){
		this.acceptorWorking = false;
	}
	
	public void destroyNow(){
		this.stop();
		
		for(Reader r : readers){
			r.stop();
		}
		for(Writer w : writers){
			w.stop();
		}
	}
	
	class AcceptorWorker implements Runnable {

		@Override
		public void run() {
			LOG.info("Started Acceptor worker: {}", Thread.currentThread().getName());
			int readerIdx = 0;
			int totalReaders = readers.length;
			
			while(acceptorWorking){
				try{
					int readyChannels = acceptorSelector.select();
					while(readyChannels == 0){
						continue;
					}
					Set<SelectionKey> selectedKeys = acceptorSelector.selectedKeys();
					Iterator<SelectionKey> keyIt = selectedKeys.iterator();
					while(keyIt.hasNext()){
						SelectionKey key = keyIt.next();
						
						// accept events
						if(key.isAcceptable()){
							// Accept connection and assign in a round robin fashion to readers
							SocketChannel incomingCon = listenerSocket.accept();
							int chosenReader = (readerIdx++)%totalReaders;
							readers[chosenReader].newConnection(incomingCon);
							readers[chosenReader].wakeUp();
						}
						if(! key.isValid()){
							LOG.warn("Acceptor key is disconnected !");
							System.exit(0);
						}
					}
					keyIt.remove();
					
				}
				catch(IOException e){
					e.printStackTrace();
				}
			}
		}
	}
	
	class Reader implements Runnable {

		private int id;
		private boolean working;
		private Queue<SocketChannel> pendingConnections;
		
		private Selector readSelector;
		
		Reader(int id, int totalNumberOfPendingConnectionsPerThread){
			this.id = id;
			this.working = true;
			this.pendingConnections = new ArrayDeque<SocketChannel>(totalNumberOfPendingConnectionsPerThread);
			try {
				this.readSelector = Selector.open();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public int id(){
			return id;
		}
		
		public void stop(){
			this.working = false;
			// TODO: more stuff here
		}
		
		public void newConnection(SocketChannel incomingChannel){
			this.pendingConnections.add(incomingChannel);
			LOG.info("New pending connection for Reader to configure");
		}
		
		public void wakeUp(){
			this.readSelector.wakeup();
		}
		
		@Override
		public void run() {
			LOG.info("Started Reader worker: {}", Thread.currentThread().getName());
			while(working){
				// First handle potential new connections that have been queued up
				this.handleNewConnections();
				try {
					int readyChannels = readSelector.select();
					if(readyChannels == 0){
						continue;
					}
					Set<SelectionKey> selectedKeys = readSelector.selectedKeys();
					Iterator<SelectionKey> keyIt = selectedKeys.iterator();
					while(keyIt.hasNext()){
						SelectionKey key = keyIt.next();
						keyIt.remove();
						// read
						if(key.isReadable()){
							if(needsToConfigureConnection(key)){
								handleConnectionIdentifier(key);
							}
							else{
								InputAdapter ia = (InputAdapter)key.attachment();
								SocketChannel channel = (SocketChannel) key.channel();
								InputBuffer buffer = ia.getInputBuffer();
								buffer.readFrom(channel, ia);
							}
						}
						if(! key.isValid()){
							String conn = ((SocketChannel)key.channel()).socket().getRemoteSocketAddress().toString();
							LOG.warn("Invalid incoming data connection to: {}", conn);
						}
					}
				}
				catch(IOException ioe){
					ioe.printStackTrace();
				}
			}
		}
		
		private boolean needsToConfigureConnection(SelectionKey key){
			return !(key.attachment() instanceof InputAdapter);
		}
		
		private boolean handleConnectionIdentifier(SelectionKey key){
			boolean moreConnectionsPending = true;
			ByteBuffer dst = ByteBuffer.allocate(100);
			try {
				int readBytes = ((SocketChannel)key.channel()).read(dst);
				if(readBytes != Type.INT.sizeOf(null)){
					// TODO: throw some type of error
				}
			} 
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			dst.flip();
			int id = dst.getInt();
			LOG.info("Received conn identifier: {}", id);
			Map<Integer, InputAdapter> iapMap = (Map<Integer, InputAdapter>)key.attachment();
			LOG.info("Configuring InputAdapter for received conn identifier: {}", id);
			InputAdapter responsibleForThisChannel = iapMap.get(id);
			if(responsibleForThisChannel == null){
				// TODO: throw exception
				LOG.error("Problem hre, no existent inputadapter");
				System.exit(0);
			}
			numUpstreamConnections--;
			if(numUpstreamConnections == 0){
				moreConnectionsPending =  false;
			}
			// Once we've identified the inputAdapter responsible for this channel we attach the new object
			key.attach(null);
			key.attach(responsibleForThisChannel);
			return moreConnectionsPending;
		}
		
		private void handleNewConnections(){
			SocketChannel incomingCon = null;
			while((incomingCon = this.pendingConnections.poll()) != null){
				try{
					incomingCon.configureBlocking(false);
					incomingCon.socket().setTcpNoDelay(true);
					// register new incoming connection in the thread-local selector
					SelectionKey key = incomingCon.register(readSelector, SelectionKey.OP_READ);
					// We attach the inputAdapterProvider Map, so that we can identify the channel once it starts
					key.attach(iapMap);
					LOG.info("Configured new incoming connection at: {}", incomingCon.toString());
				}
				catch(SocketException se){
					se.printStackTrace();
				}
				catch(IOException ioe){
					ioe.printStackTrace();
				}
			}
		}
	}
	
	class Writer implements Runnable {
		
		private int id;
		private boolean working;
		private Queue<OutputBuffer> pendingConnections;
		
		private Selector writeSelector;
		
		Writer(int id){
			this.id = id;
			this.working = true;
			this.pendingConnections = new ArrayDeque<OutputBuffer>();
			try {
				this.writeSelector = Selector.open();
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public int id(){
			return id;
		}
		
		public void stop(){
			this.working = false;
			// TODO: more stuff here
		}
		
		public void newConnection(OutputBuffer ob){
			this.pendingConnections.add(ob);
		}
		
		@Override
		public void run(){
			LOG.info("Started Writer worker: {}", Thread.currentThread().getName());
			boolean needsToSendIdentifier = true;
			boolean ongoingWrite = false;
			while(working){
				// First handle potential new connections that have been queued up
				handleNewConnections();
				try {
					int readyChannels = writeSelector.select();
					if(readyChannels == 0){
						continue;
					}
					Set<SelectionKey> selectedKeys = writeSelector.selectedKeys();
					Iterator<SelectionKey> keyIt = selectedKeys.iterator();
					while(keyIt.hasNext()) {
						SelectionKey key = keyIt.next();
						keyIt.remove();
						// connectable
						if(key.isConnectable()){
							SocketChannel sc = (SocketChannel) key.channel();
							sc.finishConnect();
							int interest = SelectionKey.OP_WRITE;
							key.interestOps(interest); // as soon as it connects it can write the init protocol
							LOG.info("Established output connection to: {}", sc.toString());
						}
						// writable
						if(key.isWritable()){
							OutputBuffer ob = (OutputBuffer)key.attachment();
							SocketChannel channel = (SocketChannel)key.channel();
							
							if(needsToSendIdentifier){
								handleSendIdentifier(ob.getStreamId(), channel);
								unsetWritable(key);
								needsToSendIdentifier = false;
							}
							else{
								synchronized(key){
									boolean fullyWritten = write(ob, channel, ongoingWrite);
									if(fullyWritten){
										// only remove interest if fully written, otherwise keep pushing the socket buffer
										ongoingWrite = false;
										unsetWritable(key);
									}
									else{
										ongoingWrite = true; // complete write next iteration
									}
								}
							}
						}
						if(! key.isValid()){
							String conn = ((SocketChannel)key.channel()).socket().getRemoteSocketAddress().toString();
							LOG.warn("Invalid outgoing data connection to: {}", conn);
						}
					}
				}
				catch(IOException ioe){
					ioe.printStackTrace();
				}
			}
		}
		
		private boolean write(OutputBuffer ob, SocketChannel channel, boolean ongoingWrite){
			ByteBuffer buf = ob.getBuffer();
			if(! ongoingWrite){
				// Write data into buffer
				buf.position(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
				List<byte[]> dataForBatch = ob.bq.poll();
				int numTuples = 0;
				int batchSize = 0;
				for(int i = 0; i<dataForBatch.size(); i++){
					byte[] data = dataForBatch.get(i);
					int tupleSize = data.length;
					buf.putInt(data.length);
					buf.put(data);
					numTuples++;
					batchSize = batchSize + tupleSize + TupleInfo.TUPLE_SIZE_OVERHEAD;
				}
				int position = buf.position();
				buf.position(TupleInfo.NUM_TUPLES_BATCH_OFFSET);
				buf.putInt(numTuples);
				buf.putInt(batchSize);
				buf.position(position);
				buf.flip(); // get buffer ready to be copied
			}
			
			// Copy buffer to channel
			int totalBytesToWrite = buf.remaining();
			int writtenBytes = 0;
			try {
				writtenBytes = channel.write(buf);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			if(writtenBytes == totalBytesToWrite){
				buf.clear();
				return true;
			}
			else{
				return false;
			}
		}
		
		private void handleSendIdentifier(int streamId, SocketChannel channel){
			ByteBuffer bb = ByteBuffer.allocate(Integer.SIZE);
			Type.INT.write(bb, streamId);
			bb.flip();
			try {
				int writtenBytes = channel.write(bb);
				if(writtenBytes != Type.INT.sizeOf(null)){
					// TODO: throw some type of error
				}
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
			LOG.info("Sent connection identifier: {}", streamId);
		}
		
		private void unsetWritable(SelectionKey key){
			final int newOps = key.interestOps() & ~SelectionKey.OP_WRITE;
			key.interestOps(newOps);
		}
		
		private void handleNewConnections(){
			try {
				OutputBuffer ob = null;
				while((ob = this.pendingConnections.poll()) != null){
					Connection c = ob.getConnection();
					SocketChannel channel = SocketChannel.open();
					InetSocketAddress address = c.getInetSocketAddressForData();
					
			        Socket socket = channel.socket();
			        socket.setKeepAlive(true); // Unlikely in non-production scenarios we'll be up for more than 2 hours but...
			        socket.setTcpNoDelay(true); // Disabling Nagle's algorithm
			        try {
			        	channel.configureBlocking(false);
			            channel.connect(address);
			        } 
			        catch (UnresolvedAddressException uae) {
			            channel.close();
			            //throw new IOException("The provided address cannot be resolved: " + address, uae);
			            uae.printStackTrace();
			        }
			        catch (IOException io) {
			            channel.close();
			            io.printStackTrace();
			        }
					channel.configureBlocking(false);
					int interestSet = SelectionKey.OP_CONNECT;
					SelectionKey key = channel.register(writeSelector, interestSet);
					key.attach(ob);
					LOG.info("Configured new output connection for {} to {}", ob.id(), address.toString());
					// Associate id - key in the networkSelectorMap
					writerKeys.put(ob.id(), key);
				}
			}
			catch(IOException io){
				io.printStackTrace();
			}
		}
	}
}