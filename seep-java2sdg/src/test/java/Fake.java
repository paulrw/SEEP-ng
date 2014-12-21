import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.annotations.Global;
import uk.ac.imperial.lsds.seep.api.annotations.Partitioned;


public class Fake {

	@Partitioned
	private int iteration;
	
	public double train(){
		iteration = 5;
		List<Double> weights = new ArrayList<Double>();
		for(int i = 0; i < iteration; i++){
			weights.add((double) (i*8));
			@Global
			double gradient = 4*5;
		}
		return weights.get(0);
	}
	
}
