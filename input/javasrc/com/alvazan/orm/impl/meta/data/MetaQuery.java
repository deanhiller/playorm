package com.alvazan.orm.impl.meta.data;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi.index.SpiMetaQuery;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;

public class MetaQuery<T> {

	private Map<String,MetaQueryFieldInfo> parameterFieldMap = new HashMap<String, MetaQueryFieldInfo>();
	
	private SpiMetaQuery spiMetaQuery;
	private MetaQueryClassInfo metaClass;
	private String query;
	
	
	@Override
	public String toString() {
		return "[Query on entity="+metaClass+" query="+query+"]";
	}

	public void initialize(MetaQueryClassInfo metaClass2, String query, SpiMetaQuery factory) {
		this.metaClass = metaClass2;
		this.query = query;
		this.spiMetaQuery = factory;
	}
	
	public MetaQueryClassInfo getMetaClass() {
		return metaClass;
	}
	


	public String getQuery() {
		return query;
	}

	public MetaQueryFieldInfo getMetaFieldByParameter(String parameter){
		return getParameterFieldMap().get(parameter);
	}

	public Map<String,MetaQueryFieldInfo> getParameterFieldMap() {
		return parameterFieldMap;
	}

	public SpiQueryAdapter createSpiMetaQuery(String indexName) {
		return spiMetaQuery.createQueryInstanceFromQuery(indexName);
	}

}
