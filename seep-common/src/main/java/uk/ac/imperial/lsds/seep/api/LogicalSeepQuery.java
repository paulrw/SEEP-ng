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

package uk.ac.imperial.lsds.seep.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LogicalSeepQuery {
	
	final private Logger LOG = LoggerFactory.getLogger(LogicalSeepQuery.class);
	
	private List<Operator> logicalOperators = new ArrayList<>();
	private List<Operator> sources = new ArrayList<>();
	private Operator sink;
	private List<LogicalState> logicalStates = new ArrayList<>();
	private Map<Integer, Integer> initialPhysicalInstancesPerOperator = new HashMap<>();
	
	public List<Operator> getAllOperators(){
		return logicalOperators;
	}
	
	public Operator getOperatorWithId(int opId){
		for(Operator lo : logicalOperators){
			if(lo.getOperatorId() == opId)
				return lo;
		}
		return null;
	}
	
	public List<LogicalState> getAllStates(){
		return logicalStates;
	}
	
	public List<Operator> getSources(){
		return sources;
	}
	
	public Operator getSink(){
		return sink;
	}
	
	public void setInitialPhysicalInstancesPerLogicalOperator(int opId, int numInstances){
		this.initialPhysicalInstancesPerOperator.put(opId, numInstances);
	}
	
	public int getInitialPhysicalInstancesForLogicalOperator(int opId){
		if (initialPhysicalInstancesPerOperator.containsKey(opId))
			return initialPhysicalInstancesPerOperator.get(opId);
		else
			return 1; // there is always, at least one instance per defined operator
	}
	
	public boolean hasSetInitialPhysicalInstances(int opId){
		return initialPhysicalInstancesPerOperator.containsKey(opId);
	}
	
	public LogicalOperator newStatefulSource(SeepTask seepTask, LogicalState state, int opId){
		LogicalOperator lo = newStatefulOperator(seepTask, state, opId);
		this.sources.add(lo);
		return lo;
	}
	
	public LogicalOperator newStatelessSource(SeepTask seepTask, int opId){
		LogicalOperator lo = newStatelessOperator(seepTask, opId);
		this.sources.add(lo);
		return lo;
	}
	
	public LogicalOperator newStatefulOperator(SeepTask seepTask, LogicalState state, int opId){
		LogicalOperator lo = SeepQueryLogicalOperator.newStatefulOperator(opId, seepTask, state);
		logicalOperators.add(lo);
		logicalStates.add(state);
		return lo;
	}
	
	public LogicalOperator newStatelessOperator(SeepTask seepTask, int opId){
		LogicalOperator lo = SeepQueryLogicalOperator.newStatelessOperator(opId, seepTask);
		logicalOperators.add(lo);
		return lo;
	}
	
	public LogicalOperator newStatefulSink(SeepTask seepTask, LogicalState state, int opId){
		LogicalOperator lo = newStatefulOperator(seepTask, state, opId);
		this.sink = lo;
		return lo;
	}
	
	public LogicalOperator newStatelessSink(SeepTask seepTask, int opId){
		LogicalOperator lo = newStatelessOperator(seepTask, opId);
		this.sink = lo;
		return lo;
	}
	
	public LogicalState newLogicalState(SeepState state, int ownerId){
		return SeepQueryOperatorState.newState(state, ownerId);
	}
	
	@Override
	public String toString(){
		String ls = System.getProperty("line.separator");
		StringBuilder sb = new StringBuilder();
		sb.append("Seep Query");
		sb.append(ls);
		sb.append("##########");
		sb.append(ls);
		sb.append("#Sources: "+this.sources.size());
		sb.append(ls);
		sb.append("#Operators(including-sources): "+this.logicalOperators.size());
		sb.append(ls);
		sb.append("#States: "+this.logicalStates.size());
		sb.append(ls);
		return sb.toString();
	}
}
