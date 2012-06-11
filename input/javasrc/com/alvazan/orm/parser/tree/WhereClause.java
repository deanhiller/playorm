package com.alvazan.orm.parser.tree;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author Huai Jiang
 *
 */
public class WhereClause implements Node {

	private Map<String,String> parameterMap = new HashMap<String, String>();
	
	
	
	private FilterExpression expression;



	public Map<String, String> getParameterMap() {
		return parameterMap;
	}



	public void setParameterMap(Map<String, String> parameterMap) {
		this.parameterMap = parameterMap;
	}



	public FilterExpression getExpression() {
		return expression;
	}



	public void setExpression(FilterExpression expression) {
		this.expression = expression;
	}
	
	
	
}
