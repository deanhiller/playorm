package com.alvazan.ssql.cmdline;

public class Key {

	private Integer index;
	private String colName;

	public Key(Integer index, String colName) {
		this.index = index;
		this.colName = colName;
	}
	public Integer getIndex() {
		return index;
	}
	public String getColName() {
		return colName;
	}
	
	
}
