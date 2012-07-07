package com.alvazan.orm.impl.meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.Query;
import com.alvazan.orm.layer1.base.QueryAdapter;
import com.alvazan.orm.layer3.spi.index.SpiMetaQuery;
import com.alvazan.orm.layer3.spi.index.SpiQueryAdapter;

public class MetaQuery<T> {

	private List<MetaQueryFieldInfo> projectionFields = new ArrayList<MetaQueryFieldInfo>();
	
	private Map<String,MetaQueryFieldInfo> parameterFieldMap = new HashMap<String, MetaQueryFieldInfo>();
	
	@Inject
	private Provider<QueryAdapter> adapterFactory;
	
	private SpiMetaQuery<T> spiMetaQuery;
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

	List<MetaQueryFieldInfo> getProjectionFields() {
		return projectionFields;
	}

	Map<String,MetaQueryFieldInfo> getParameterFieldMap() {
		return parameterFieldMap;
	}

	@SuppressWarnings("unchecked")
	public Query<T> createAdapter(String indexName) {
		//We cannot return MetaQuery since it is used by all QueryAdapters and each QueryAdapter
		//runs in a different thread potentially while MetaQuery is one used by all threads
		QueryAdapter<T> adapter = adapterFactory.get();
		SpiQueryAdapter<T> indexQuery = spiMetaQuery.createQueryInstanceFromQuery(indexName);
		adapter.setup(this, indexQuery);
		return adapter;
	}

}
