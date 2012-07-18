package com.alvazan.orm.api.spi.layer2;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi.index.SpiMetaQuery;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;

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

	public void initialize(String query, SpiMetaQuery factory) {
		this.query = query;
		this.spiMetaQuery = factory;
	}
	
	public String getQuery() {
		return query;
	}

	public TypeInfo getMetaFieldByParameter(String parameter){
		return getParameterFieldMap().get(parameter);
	}

	public Map<String,TypeInfo> getParameterFieldMap() {
		return parameterFieldMap;
	}

	public SpiQueryAdapter createSpiMetaQuery(String indexName) {
		return spiMetaQuery.createQueryInstanceFromQuery(indexName);
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
		this.indexName = indexName;
	}
	
}
