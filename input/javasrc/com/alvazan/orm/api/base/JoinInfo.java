package com.alvazan.orm.api.base;

public class JoinInfo {
	private Class<?> joinClassType;
	private String indexNameToJoin;
	public Class<?> getJoinClassType() {
		return joinClassType;
	}
	public void setJoinClassType(Class<?> joinClassType) {
		this.joinClassType = joinClassType;
	}
	public String getIndexNameToJoin() {
		return indexNameToJoin;
	}
	public void setIndexNameToJoin(String indexNameToJoin) {
		this.indexNameToJoin = indexNameToJoin;
	}
	
	
}
