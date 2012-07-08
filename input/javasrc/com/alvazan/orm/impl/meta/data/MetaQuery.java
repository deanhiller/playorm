package com.alvazan.orm.impl.meta.data;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi.index.SpiMetaQuery;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;
import com.alvazan.orm.impl.meta.query.MetaColumnDbo;
import com.alvazan.orm.impl.meta.query.MetaTableDbo;

public class MetaQuery<T> {

	private Map<String,MetaColumnDbo> parameterFieldMap = new HashMap<String, MetaColumnDbo>();
	
	private SpiMetaQuery spiMetaQuery;
	private String query;

	//NOTE: This is really JUST for ad-hoc query tool
	private String indexName;

	private MetaTableDbo targetTable;
	
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

	public MetaColumnDbo getMetaFieldByParameter(String parameter){
		return getParameterFieldMap().get(parameter);
	}

	public Map<String,MetaColumnDbo> getParameterFieldMap() {
		return parameterFieldMap;
	}

	public SpiQueryAdapter createSpiMetaQuery(String indexName) {
		return spiMetaQuery.createQueryInstanceFromQuery(indexName);
	}

	public String getIndexName() {
		return indexName;
	}

	public void setTargetTable(MetaTableDbo metaClass) {
		this.targetTable = metaClass;
	}

	public MetaTableDbo getTargetTable() {
		return targetTable;
	}
	
}
