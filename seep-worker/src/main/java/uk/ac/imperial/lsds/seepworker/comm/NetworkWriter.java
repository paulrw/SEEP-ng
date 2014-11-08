package uk.ac.imperial.lsds.seepworker.comm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;
import uk.ac.imperial.lsds.seepworker.core.output.Writer;

public class NetworkWriter implements Writer {

	private Selector selector;
	private int streamId;
	
	private boolean working = false;
	private Thread worker;
	
	// this will actually receive whatever objects we want to attach to the key
	public NetworkWriter(int streamId, Map<Integer, OutputBuffer> outputBuffers, Selector selector){
		this.streamId = streamId;
		try{
			for(OutputBuffer ob : outputBuffers.values()){	
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
				SelectionKey key = channel.register(selector, interestSet);
				key.attach(ob);
			}
		}
		catch(IOException io){
			io.printStackTrace();
		}
		worker = new Thread(new Worker());
		worker.setName(this.getClass().getName());
	}
	
	@Override
	public void start() {
		this.working = true;
		worker.start();
		// TODO: set uncaught exception handler, etc...
	}

	@Override
	public void stop() {
		working = false;
		// TODO: cleaning stuff, etc
	}
	
	private void setWritable(SelectionKey key){
		final int newOps = key.interestOps() & ~SelectionKey.OP_WRITE;
		key.interestOps(newOps);
	}
	
	private void unsetWritable(SelectionKey key){
		final int newOps = key.interestOps() & ~SelectionKey.OP_WRITE;
		key.interestOps(newOps);
	}
	
	class Worker implements Runnable{

		@Override
		public void run() {
			while(working){
				try {
					int readyChannels = selector.select();
					while(readyChannels == 0){ 
						continue;
					}
					Set<SelectionKey> selectedKeys = selector.selectedKeys();
					Iterator<SelectionKey> keyIt = selectedKeys.iterator();
					while(keyIt.hasNext()){
						SelectionKey key = keyIt.next();
						
						OutputBuffer ob = (OutputBuffer)key.attachment();
						SocketChannel channel = (SocketChannel) key.channel();
						
						// connect events
						if(key.isConnectable()){
							SocketChannel sc = (SocketChannel) key.channel();
							sc.finishConnect();
							key.interestOps(SelectionKey.OP_WRITE);
						}
						// write events
						if(key.isWritable()){
							// get attachment object (would be the active channel) 
							// and write what we have in the buffer
							channel.write(ob.getBuffer());
							unsetWritable(key);
						}
					}
					
					keyIt.remove();
				} 
				catch (IOException e) {
					e.printStackTrace();
				}	
			}
		}
		
	}
	
}
