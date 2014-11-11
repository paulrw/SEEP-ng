package uk.ac.imperial.lsds.seepworker.comm;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
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

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.Main;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.InputBuffer;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;

public class NetworkSelector implements EventAPI {

	final private static Logger LOG = LoggerFactory.getLogger(NetworkSelector.class);
	
	private Selector acceptorSelector;
	
	private ServerSocketChannel listenerSocket;
	
	private boolean working = false;
	private Thread worker;
	
	private Reader[] readers;
	private Writer[] writers;
	private int numReaderWorkers;
	private int totalNumberPendingConnectionsPerThread;
	
	private Thread[] readerWorkers;
	private Thread[] writerWorkers;
	private int numWriterWorkers;
	
	private Map<Integer, SelectionKey> writerKeys;
	
	public NetworkSelector(WorkerConfig wc) {
		this.numReaderWorkers = wc.getInt(WorkerConfig.NUM_NETWORK_READER_THREADS); 
		this.numWriterWorkers = wc.getInt(WorkerConfig.NUM_NETWORK_WRITER_THREADS);
		this.totalNumberPendingConnectionsPerThread = wc.getInt(WorkerConfig.MAX_PENDING_NETWORK_CONNECTION_PER_THREAD);
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
		int interestOps = key.interestOps() | SelectionKey.OP_WRITE;
		key.interestOps(interestOps);
		key.selector().wakeup();
	}
	
	@Override
	public void readyForWrite(List<Integer> ids){
		for(Integer id : ids){
			SelectionKey key = writerKeys.get(id);
			int interestOps = key.interestOps() | SelectionKey.OP_WRITE;
			key.interestOps(interestOps);
			key.selector().wakeup();
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
	}
	
	public void configureConnect(Set<OutputBuffer> obufs){
		int writerIdx = 0;
		int totalWriters = writers.length;
		for(OutputBuffer obuf : obufs){
			writers[(writerIdx++)%totalWriters].newConnection(obuf);
		}
	}
	
	public void start(){
		this.working = true;
		this.worker.start();
	}
	
	public void stop(){
		this.working = false;
	}
	
	class AcceptorWorker implements Runnable {

		@Override
		public void run() {
			
			int readerIdx = 0;
			int totalReaders = readers.length;
			
			while(working){
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
							readers[(readerIdx++)%totalReaders].newConnection(incomingCon);
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
				// TODO Auto-generated catch block
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
		}
		
		@Override
		public void run() {
			while(working){
				// First handle potential new connections that have been queued up
				handleNewConnections();
				try {
					int readyChannels = readSelector.select();
					while(readyChannels == 0){
						continue;
					}
					Set<SelectionKey> selectedKeys = readSelector.selectedKeys();
					Iterator<SelectionKey> keyIt = selectedKeys.iterator();
					while(keyIt.hasNext()){
						SelectionKey key = keyIt.next();
						if(key.isReadable()){
							InputBuffer ib = (InputBuffer)key.attachment();
							SocketChannel channel = (SocketChannel) key.channel();
							ib.networkRead(channel);
						}
					}
					keyIt.remove();
				}
				catch(IOException ioe){
					ioe.printStackTrace();
				}
			}
		}
		
		private void handleNewConnections(){
			SocketChannel incomingCon = null;
			while((incomingCon = pendingConnections.poll()) != null){
				try{
					incomingCon.configureBlocking(false);
					incomingCon.socket().setTcpNoDelay(true);
					// register new incoming connection in the thread-local selector
					incomingCon.register(readSelector, SelectionKey.OP_READ, new InputBuffer());
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
			while(working){
				// First handle potential new connections that have been queued up
				handleNewConnections();
				try {
					int readyChannels = writeSelector.select();
					while(readyChannels == 0){
						continue;
					}
					Set<SelectionKey> selectedKeys = writeSelector.selectedKeys();
					Iterator<SelectionKey> keyIt = selectedKeys.iterator();
					while(keyIt.hasNext()){
						SelectionKey key = keyIt.next();
						
						// connectable
						if(key.isConnectable()){
							SocketChannel sc = (SocketChannel) key.channel();
							sc.finishConnect();
							key.interestOps(SelectionKey.OP_WRITE);
						}
						// writable
						if(key.isWritable()){
							OutputBuffer ob = (OutputBuffer)key.attachment();
							SocketChannel channel = (SocketChannel)key.channel();
							channel.write(ob.getBuffer());
							unsetWritable(key);
						}
					}
					keyIt.remove();
				}
				catch(IOException ioe){
					ioe.printStackTrace();
				}
			}
		}
		
		private void unsetWritable(SelectionKey key){
			final int newOps = key.interestOps() & ~SelectionKey.OP_WRITE;
			key.interestOps(newOps);
		}
		
		private void handleNewConnections(){
			try {
				for(OutputBuffer ob : this.pendingConnections){
					Connection c = ob.getConnection();
					SocketChannel channel = SocketChannel.open();
					InetSocketAddress address = c.getInetSocketAddress();
					
			        Socket socket = channel.socket();
			        socket.setKeepAlive(true); // Unlikely in non-production scenarios we'll be up for more than 2 hours but...
			        socket.setTcpNoDelay(true); // Disabling Nagle's algorithm
			        try {
			            channel.connect(address);
			        } 
			        catch (UnresolvedAddressException uae) {
			            channel.close();
			            throw new IOException("The provided address cannot be resolved: " + address, uae);
			        }
			        catch (IOException io) {
			            channel.close();
			            throw io;
			        }
					channel.configureBlocking(false);
					int interestSet = SelectionKey.OP_CONNECT;
					SelectionKey key = channel.register(writeSelector, interestSet);
					key.attach(ob);
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