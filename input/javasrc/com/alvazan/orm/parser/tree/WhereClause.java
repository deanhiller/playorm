package com.alvazan.orm.parser.tree;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author Huai Jiang
 *
 */
public class WhereClause implements Node {

	
	//This is a cache for whole where clause, otherwise we have go to expression tree everytime to get the attribute and parameter
	//Using FilterAttribute and Parameter  instead of Map<String, String> because same attribute could exist in expression in N times
	private Map<Attribute,FilterParameter> parameterMap = new HashMap<Attribute, FilterParameter>();
	
	
	
	private FilterExpression expression;



	public Map<Attribute, FilterParameter> getParameterMap() {
		return parameterMap;
	}




	public FilterExpression getExpression() {
		return expression;
	}



	public void setExpression(FilterExpression expression) {
		this.expression = expression;
	}
	
	
	
}
