package com.alvazan.orm.layer5.query;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.z5api.MetaQuery;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z5api.SpiQueryAdapter;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypeInfo;

public class MetaQueryImpl<T> implements MetaQuery<T> {

	private Map<String,TypeInfo> parameterFieldMap = new HashMap<String, TypeInfo>();
	
	private SpiMetaQuery spiMetaQuery;
	private String query;

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
