package com.alvazan.orm.parser.tree;

public class FilterParameter implements Node{

	
	private String parameter;

	public String getParameter() {
		return parameter;
	}

	public void setParameter(String parameter) {
		this.parameter = parameter;
	}
	
	@Override
	public String toString() {
		return "Parameter:["+this.parameter+"]";
	}
}
