/*******************************************************************************
 * Copyright (c) 2013 Imperial College London.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Raul Castro Fernandez - initial design and implementation
 ******************************************************************************/
package uk.ac.imperial.lsds.seepworker;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

import joptsimple.OptionParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seep.comm.serialization.JavaSerializer;
import uk.ac.imperial.lsds.seep.config.CommandLineArgs;
import uk.ac.imperial.lsds.seep.config.ConfigKey;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.RuntimeClassLoader;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.comm.WorkerMasterAPIImplementation;
import uk.ac.imperial.lsds.seepworker.comm.WorkerMasterCommManager;
import uk.ac.imperial.lsds.seepworker.comm.WorkerWorkerAPIImplementation;
import uk.ac.imperial.lsds.seepworker.comm.WorkerWorkerCommManager;
import uk.ac.imperial.lsds.seepworker.core.Conductor;


public class Main {
	
	final private static Logger LOG = LoggerFactory.getLogger(Main.class);
	
	private void executeWorker(WorkerConfig wc){
		int masterPort = wc.getInt(WorkerConfig.MASTER_PORT);
		InetAddress masterIp = null;
		try {
			masterIp = InetAddress.getByName(wc.getString(WorkerConfig.MASTER_IP));
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		// Get connection to master node
		int masterId = Utils.computeIdFromIpAndPort(masterIp, masterPort);
		Connection masterConnection = new Connection(new EndPoint(masterId, masterIp, masterPort));
		
		int myPort = wc.getInt(WorkerConfig.LISTENING_PORT);
		int dataPort = wc.getInt(WorkerConfig.DATA_PORT);
		
		// Create workerMaster comm manager
		Comm comm = new IOComm(new JavaSerializer(), Executors.newCachedThreadPool());
		
		// Create conductor
		Conductor c = new Conductor(Utils.getLocalIp(), wc);
		
		// Start master-worker communication manager
		WorkerMasterAPIImplementation api = new WorkerMasterAPIImplementation(comm, c, wc);
		RuntimeClassLoader rcl = new RuntimeClassLoader(new URL[0], this.getClass().getClassLoader());
		WorkerMasterCommManager wmcm = new WorkerMasterCommManager(myPort, api, rcl);
		wmcm.start();
		
		// Start worker-worker communication manager
		WorkerWorkerAPIImplementation apiWorker = new WorkerWorkerAPIImplementation(comm, c, wc);
		int wwPort = 0; // TODO: get this somehow...
		WorkerWorkerCommManager wwcm = new WorkerWorkerCommManager(wwPort, apiWorker);
		wwcm.start();
		
		// bootstrap
		String myIp = Utils.getStringRepresentationOfLocalIp();
		api.bootstrap(masterConnection, myIp, myPort, dataPort);
	}
	
	public static void main(String args[]){
		
		// Get properties from command line
		List<ConfigKey> configKeys = WorkerConfig.getAllConfigKey();
		OptionParser parser = new OptionParser();
		CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
		Properties commandLineProperties = cla.getProperties();
		
		// Get properties from file, if any
		Properties fileProperties = Utils.readPropertiesFromFile(WorkerConfig.PROPERTIES_FILE, WorkerConfig.PROPERTIES_RESOURCE_FILE);
		
		Properties validatedProperties = Utils.overwriteSecondPropertiesWithFirst(commandLineProperties, fileProperties);
		boolean validates = validateProperties(validatedProperties);
		if(!validates){
			printHelp(parser);
			System.exit(0);
		}
		
		WorkerConfig wc = new WorkerConfig(validatedProperties);
		Main instance = new Main();
		instance.executeWorker(wc);
	}
	
	private static boolean validateProperties(Properties validatedProperties){
		if((!validatedProperties.containsKey(WorkerConfig.MASTER_IP)) ||
				validatedProperties.getProperty(WorkerConfig.MASTER_IP) == null ||
				validatedProperties.getProperty(WorkerConfig.MASTER_IP).equals("")){
			LOG.error("Missing required parameter: {}", WorkerConfig.MASTER_IP);
			return false;
		}
			
		return true;
	}
	
	private static void printHelp(OptionParser parser){
		try {
			parser.printHelpOn(System.out);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
