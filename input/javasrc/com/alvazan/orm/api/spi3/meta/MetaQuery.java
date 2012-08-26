package com.alvazan.orm.api.spi3.meta;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi5.SpiMetaQuery;
import com.alvazan.orm.api.spi5.SpiQueryAdapter;


public class MetaQuery<T> {

	private Map<String,TypeInfo> parameterFieldMap = new HashMap<String, TypeInfo>();
	
	private SpiMetaQuery spiMetaQuery;
	private String query;

	//NOTE: This is really JUST for ad-hoc query tool
	private String indexName;

	private DboTableMeta targetTable;
	
	@Override
	public String toString() {
		return "[Query="+query+"]";
	}

	public void initialize(String query) {
		this.query = query;
	}
	
	public void initialize(String query, SpiMetaQuery factory) {
		this.query = query;
		this.spiMetaQuery = factory;
	}

	public TypeInfo getMetaFieldByParameter(String parameter){
		return parameterFieldMap.get(parameter);
	}

	public SpiQueryAdapter createSpiMetaQuery(NoSqlSession session) {
		return spiMetaQuery.createQueryInstanceFromQuery(session);
	}

	public String getIndexName() {
		return indexName;
	}

	public void setTargetTable(DboTableMeta metaClass) {
		this.targetTable = metaClass;
	}

	public DboTableMeta getTargetTable() {
		return targetTable;
	}

	public void setIndexName(String indexName) {
		throw new UnsupportedOperationException("not supported, fix this");
	}

	public String getQuery() {
		return query;
	}

	public void setParameterFieldMap(Map<String, TypeInfo> parameterFieldMap2) {
		this.parameterFieldMap = parameterFieldMap2;
	}
	
}
