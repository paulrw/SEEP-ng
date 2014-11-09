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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.core.input.InputBuffer;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;

public class NetworkSelector {

	private Selector s;
	
	private ServerSocketChannel listenerSocket;
	private List<SocketChannel> outputSockets;
	
	private boolean working = false;
	private Thread worker;
	
	public NetworkSelector(){
		try {
			this.s = Selector.open();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// This guy receives channels that have already been configured
	public NetworkSelector(ServerSocketChannel listenerSocket, List<SocketChannel> outputSockets){
		this.listenerSocket = listenerSocket;
		this.outputSockets = outputSockets;
		try {
			listenerSocket.register(s, SelectionKey.OP_ACCEPT);
			for(SocketChannel sc : outputSockets){
				sc.register(s, SelectionKey.OP_CONNECT);
			}
		}
		catch (ClosedChannelException e) {
			e.printStackTrace();
		}
		
		this.worker = new Thread(new SelectorWorker());
		this.worker.setName(SelectorWorker.class.getName());
	}
	
	public void configureAccept(InetAddress myIp, int dataPort, Object o){
		ServerSocketChannel channel = null;
		try {
			channel = ServerSocketChannel.open();
			SocketAddress sa = new InetSocketAddress(myIp, dataPort);
			channel.configureBlocking(false);
			channel.bind(sa);
			SelectionKey key = channel.register(s, SelectionKey.OP_ACCEPT);
			key.attach(o);
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
		List<SocketChannel> channels = new ArrayList<>();
		try{
			for(OutputBuffer ob : obufs){	
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
				SelectionKey key = channel.register(s, interestSet);
				key.attach(ob);
				channels.add(channel);
			}
		}
		catch(SocketException se){
			se.printStackTrace();
		}
		catch(IOException io){
			io.printStackTrace();
		}
		this.outputSockets = channels;
	}
	
	public void start(){
		this.working = true;
		this.worker.start();
	}
	
	public void stop(){
		this.working = false;
	}
	
	class SelectorWorker implements Runnable {

		@Override
		public void run() {
			while(working){
				try{
					int readyChannels = s.select();
					while(readyChannels == 0){ 
						continue;
					}
					Set<SelectionKey> selectedKeys = s.selectedKeys();
					Iterator<SelectionKey> keyIt = selectedKeys.iterator();
					while(keyIt.hasNext()){
						SelectionKey key = keyIt.next();
						SocketChannel channel = (SocketChannel) key.channel();
						
						// accept events
						if(key.isAcceptable()){
							SocketChannel incomingCon = listenerSocket.accept();
							incomingCon.configureBlocking(false);
							incomingCon.socket().setTcpNoDelay(true);
							// configure that inputBuffer
							incomingCon.register(s, SelectionKey.OP_READ, new InputBuffer());
						}
						// connect events
						if(key.isConnectable()){
							SocketChannel sc = (SocketChannel) key.channel();
							sc.finishConnect();
							key.interestOps(SelectionKey.OP_WRITE);
						}
						// read events
						if(key.isReadable()){
							// TODO: delegate to thread pool
							InputBuffer ib = (InputBuffer)key.attachment();
							ib.networkRead((SocketChannel)key.channel());
						}
						// write events
						if(key.isWritable()){
							// TODO: delegate to thread
							OutputBuffer ob = (OutputBuffer)key.attachment();
							channel.write(ob.getBuffer());
							
							unsetWritable(key);
						}
						
					}
					keyIt.remove();
					
				}
				catch(IOException e){
					e.printStackTrace();
				}
			}
			
		}
		
		private void unsetWritable(SelectionKey key){
			final int newOps = key.interestOps() & ~SelectionKey.OP_WRITE;
			key.interestOps(newOps);
		}
		
	}
	
}
