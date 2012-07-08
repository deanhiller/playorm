package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.Map;

import org.apache.lucene.search.Query;

public interface QueryNode {

	public QueryNode getParentNode() ;
	public void setParentNode(QueryNode parentNode) ;
	
	
	public Query getQuery(Map<String,Object> parameterValues);
}
