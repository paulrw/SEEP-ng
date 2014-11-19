/*******************************************************************************
 * Copyright (c) 2014 Imperial College London
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Panagiotis Garefalakis - Main Exporter implementation
 ******************************************************************************/
package uk.ac.imperial.lsds.java2sdg;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import soot.SootClass;
import soot.SootField;
import soot.SootMethod;
import soot.Unit;
import soot.Value;
import soot.ValueBox;
import soot.jimple.InvokeExpr;
import soot.toolkits.graph.UnitGraph;

public class MainExtractor {

	private final static Logger log = Logger.getLogger(Main.class
			.getCanonicalName());

	// private static String inputSourceType;
	private static List<String> workflowNames;

	// private static Map<String, JavaType> inputSchema;
	// private static String outputSinkType;

	public MainExtractor() {
		// inputSchema = null;
		// outputSinkType = null;
		// inputSchema = new HashMap<String, JavaType>();
		workflowNames = new ArrayList<String>();
	}

	private static List<String> extractWorkflowsFromMainMethod(SootMethod sm,
			SootClass c) {
		List<String> workflowsNames = new ArrayList<String>();
		UnitGraph cfg = Util.getCFGForMethod(sm.getName(), c);
		Iterator<Unit> units = cfg.iterator();
		while (units.hasNext()) {
			Unit u = units.next();
			Iterator<ValueBox> iValueBox = u.getUseBoxes().iterator();
			while (iValueBox.hasNext()) {
				ValueBox valueBox = iValueBox.next();
				Value v = valueBox.getValue();
				if (v instanceof InvokeExpr) {
					InvokeExpr m = (InvokeExpr) v;
					SootMethod method = m.getMethod();
					String methodName = method.getName();
					workflowsNames.add(methodName);
				}
			}
		}
		return workflowsNames;
	}

	private static String extractInputSourceFromMainMethod(
			Iterator<SootField> fieldsIterator) {

		// int sourceID=0;
		// String source;
		//
		// while(fieldsIterator.hasNext()){
		// SootField field = fieldsIterator.next();
		// Type fieldType = field.getType();
		// SootClass sc = null;
		// try{
		// sc = Scene.v().loadClassAndSupport(fieldType.toString());
		// }
		// catch(RuntimeException re){
		// log.warning("Field: "+fieldType.toString()+" is not a valid class");
		// continue;
		// }
		// System.out.println("Field:"+ field.getName());
		// Tag annotationTag = field.getTag("AnnotationElem");
		// if(annotationTag != null){
		// String rawAnnotationData = annotationTag.toString();
		// System.out.println("AnnotationElem:"+ field.getName());
		//
		//
		// if(rawAnnotationData.contains("File")){
		// source = rawAnnotationData;
		// System.out.println("Data: "+ rawAnnotationData + " SC: "+ sc);
		// System.out.println("field: "+ field.getName() );
		// sourceID++;
		// }
		// else if(rawAnnotationData.contains("Network")){
		// source = rawAnnotationData;
		// System.out.println("Data: "+ rawAnnotationData + " SC: "+ sc);
		// System.out.println("field: "+ field.getName() );
		// sourceID++;
		// }
		//
		// }
		// }
		return null;
	}

	public void extractWorkflows(Iterator<SootMethod> methods, SootClass c) {

		// First we detect the main program to analyze it

		while (methods.hasNext()) {
			SootMethod sm = methods.next();

			if (sm.getName().equals("main")) {
				log.info("Detected Main method..");

				MainExtractor.workflowNames = extractWorkflowsFromMainMethod(sm,
						c);
				// MainExporter.extractInputSourceFromMainMethod(c.getMethodByName("main").getTags().iterator());

				log.info("Main Parsing done!");
				break;
			}

		}

	}

	/**
	 * @return the inputSourceType
	 * 
	 *         public static String getInputSourceType() { return
	 *         inputSourceType; }
	 */

	/**
	 * @return the workflowNames
	 */
	public static List<String> getWorkflowNames() {
		return workflowNames;
	}

	/**
	 * @return the inputSchema
	 * 
	 *         public static Map<String, JavaType> getInputSchema() { return
	 *         inputSchema; }
	 */

	/**
	 * @return the outputSinkType
	 * 
	 *         public static String getOutputSinkType() { return outputSinkType;
	 *         }
	 */

}
