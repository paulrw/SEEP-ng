package uk.ac.imperial.lsds.seepmaster;

import joptsimple.OptionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seep.comm.serialization.JavaSerializer;
import uk.ac.imperial.lsds.seep.config.CommandLineArgs;
import uk.ac.imperial.lsds.seep.config.ConfigKey;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepmaster.comm.MasterWorkerAPIImplementation;
import uk.ac.imperial.lsds.seepmaster.comm.MasterWorkerCommManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManagerFactory;
import uk.ac.imperial.lsds.seepmaster.query.QueryManager;
import uk.ac.imperial.lsds.seepmaster.ui.UI;
import uk.ac.imperial.lsds.seepmaster.ui.UIFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;


public class Main {
	
	final private static Logger LOG = LoggerFactory.getLogger(Main.class);

	private void executeMaster(String[] args, MasterConfig mc, String[] queryArgs){
		int infType = mc.getInt(MasterConfig.DEPLOYMENT_TARGET_TYPE);
		LOG.info("Deploy target of type: {}", InfrastructureManagerFactory.nameInfrastructureManagerWithType(infType));
		InfrastructureManager inf = InfrastructureManagerFactory.createInfrastructureManager(infType);
		// TODO: get file from config if exists and parse it to get a map from operator to endPoint
		Map<Integer, EndPoint> mapOperatorToEndPoint = null;
		// TODO: from properties get serializer and type of thread pool and resources assigned to it
		Comm comm = new IOComm(new JavaSerializer(), Executors.newCachedThreadPool());
		QueryManager qm = QueryManager.getInstance(inf, mapOperatorToEndPoint, comm);
		// TODO: put this in the config manager
		int port = mc.getInt(MasterConfig.LISTENING_PORT);
		MasterWorkerAPIImplementation api = new MasterWorkerAPIImplementation(qm, inf);
		MasterWorkerCommManager mwcm = new MasterWorkerCommManager(port, api);
		mwcm.start();
		int uiType = mc.getInt(MasterConfig.UI_TYPE);
		UI ui = UIFactory.createUI(uiType, qm, inf);
		LOG.info("Created UI of type: {}", UIFactory.nameUIOfType(uiType));
		String queryPathFile = mc.getString(MasterConfig.QUERY_FILE);
		String baseClass = mc.getString(MasterConfig.BASECLASS_NAME);
		LOG.info("Loading query {} with baseClass: {} from file...", queryPathFile, baseClass);
		qm.loadQueryFromFile(queryPathFile, baseClass, queryArgs);
		LOG.info("Loading query...OK");
		ui.start();		
	}
	
	public static void main(String args[]){
		// Get Properties with command line configuration 
		List<ConfigKey> configKeys = MasterConfig.getAllConfigKey();
		OptionParser parser = new OptionParser();
		parser.allowsUnrecognizedOptions();
		parser.accepts(MasterConfig.QUERY_FILE, "Jar file with the compiled SEEP query").withRequiredArg().required();
		parser.accepts(MasterConfig.BASECLASS_NAME, "Name of the Base Class").withRequiredArg().required();
		CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
		Properties commandLineProperties = cla.getProperties();
		
		// Get Properties with file configuration
		Properties fileProperties = Utils.readPropertiesFromFile(MasterConfig.PROPERTIES_FILE, MasterConfig.PROPERTIES_RESOURCE_FILE);
		
		// Merge both properties, command line has preference
		Properties validatedProperties = Utils.overwriteSecondPropertiesWithFirst(commandLineProperties, fileProperties);
		boolean validates = validateProperties(validatedProperties);		
		if(!validates){
			printHelp(parser);
			System.exit(0);
		}
		MasterConfig mc = new MasterConfig(validatedProperties);
		Main instance = new Main();
		instance.executeMaster(args, mc, cla.getQueryArgs());
	}
	
	private static boolean validateProperties(Properties validatedProperties){
		if((!validatedProperties.containsKey(MasterConfig.QUERY_FILE)) ||
				validatedProperties.getProperty(MasterConfig.QUERY_FILE) == null ||
				validatedProperties.getProperty(MasterConfig.QUERY_FILE).equals("")){
			LOG.error("Missing required parameter: {}", MasterConfig.QUERY_FILE);
			return false;
		}
		if((!validatedProperties.containsKey(MasterConfig.BASECLASS_NAME)) ||
				validatedProperties.getProperty(MasterConfig.BASECLASS_NAME) == null ||
				validatedProperties.getProperty(MasterConfig.BASECLASS_NAME).equals("")){
			LOG.error("Missing required parameter: {}", MasterConfig.BASECLASS_NAME);
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