package com.alvazan.orm.impl.meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.layer3.spi.index.SpiIndexQuery;
import com.alvazan.orm.layer3.spi.index.SpiIndexQueryFactory;

public class MetaQuery<T> {

	private List<MetaQueryFieldInfo> projectionFields = new ArrayList<MetaQueryFieldInfo>();
	
	private Map<String,MetaQueryFieldInfo> parameterFieldMap = new HashMap<String, MetaQueryFieldInfo>();
	
	private SpiIndexQueryFactory<T> factory;
	private MetaQueryClassInfo metaClass;
	private String query;
	
	public void initialize(MetaQueryClassInfo metaClass2, String query, SpiIndexQueryFactory factory) {
		this.metaClass = metaClass2;
		this.query = query;
		this.factory = factory;
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

	public SpiIndexQuery<T> createSpiAdapter(String indexName) {
		return factory.createQuery(indexName);
	}

	List<MetaQueryFieldInfo> getProjectionFields() {
		return projectionFields;
	}

	Map<String,MetaQueryFieldInfo> getParameterFieldMap() {
		return parameterFieldMap;
	}

}
