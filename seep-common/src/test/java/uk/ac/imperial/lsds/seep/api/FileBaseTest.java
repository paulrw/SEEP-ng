package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

public class FileBaseTest implements QueryComposer {

	@Override
	public LogicalSeepQuery compose() {
		
		Schema s = SchemaBuilder.getInstance().newField(Type.LONG, "w1").newField(Type.LONG, "w2").build();
		
		LogicalOperator trainer = queryAPI.newStatelessOperator(new Trainer(), 0);
		LogicalOperator parameterServer = queryAPI.newStatelessOperator(new ParameterServer(), 1);
		
		FileSource.connectTo(trainer, 0, s);
		trainer.connectTo(parameterServer, 0, s, ConnectionType.UPSTREAM_SYNC_BARRIER);
		parameterServer.connectTo(trainer, 1, s);
		
		return queryAPI.build();
	}
	
	class Trainer implements SeepTask {
		@Override
		public void setUp() {		}
		@Override
		public void processData(ITuple data, API api) {		}
		@Override
		public void processDataGroup(ITuple dataBatch, API api) {		}
		@Override
		public void close() {		}
	}
	
	class ParameterServer implements SeepTask {
		@Override
		public void setUp() {		}
		@Override
		public void processData(ITuple data, API api) {		}
		@Override
		public void processDataGroup(ITuple dataBatch, API api) {		}
		@Override
		public void close() {		}
	}

}
