package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.sources.SimpleNetworkSource;

public class BaseTest implements QueryComposer{

	@Override
	public LogicalSeepQuery compose() {
		// Declare Source
		LogicalOperator src = queryAPI.newStatelessSource(new Source(), -1);
		
		/**
		 * Another option, coming from java2sdg
		 */
		int port = -1; // get data from java2sdg
		LogicalOperator networkSrc = queryAPI.newStatelessSource(new SimpleNetworkSource(port), 100);
		
		// Declare processor
		LogicalOperator p = queryAPI.newStatelessOperator(new Processor(), 1);
		// Declare sink
		LogicalOperator snk = queryAPI.newStatelessSink(new Sink(), -2);
		
		Schema srcSchema = queryAPI.schemaBuilder.newField(Type.SHORT, "id").build();
		Schema pSchema = queryAPI.schemaBuilder.newField(Type.SHORT, "id").newField(Type.BYTES, "payload").build();
		
		System.out.println("SRC Schema: ");
		System.out.println(srcSchema.toString());
		System.out.println("Pro Schema: ");
		System.out.println(pSchema.toString());
		
		/** Connect operators **/
		src.connectTo(p, 0, srcSchema);
		p.connectTo(snk, 0, pSchema);
		
		/**
		 * Example of how to create a source with only java2sdg information
		Schema networkSchema = ...; // got from java2sdg
		networkSrc.connectTo(p, 0, networkSchema);
		**/
		
		return QueryBuilder.build();
	}

	
	class Source implements SeepTask{
		@Override
		public void setUp() {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processData(ITuple data, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processDataGroup(ITuple dataList, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub	
		}
	}
	
	class Processor implements SeepTask{
		@Override
		public void setUp() {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processData(ITuple data, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processDataGroup(ITuple dataList, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub	
		}
	}
	
	class Sink implements SeepTask{
		@Override
		public void setUp() {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processData(ITuple data, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processDataGroup(ITuple dataList, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub	
		}
	}
}
