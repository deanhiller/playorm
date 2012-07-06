package com.alvazan.orm.impl.meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.layer3.spi.index.SpiIndexQuery;
import com.alvazan.orm.layer3.spi.index.SpiIndexQueryFactory;

@SuppressWarnings("rawtypes")
public class MetaQuery<T> {

	private static final Logger log = LoggerFactory.getLogger(MetaQuery.class);
	
	private List<MetaField> projectionFields = new ArrayList<MetaField>();
	
	private Map<MetaField, String> fieldParameterMap = new HashMap<MetaField, String>();
	
	private Map<String,MetaField<?>> parameterFieldMap = new HashMap<String, MetaField<?>>();
	private SpiIndexQueryFactory<T> factory;
	
	public void setup(SpiIndexQueryFactory<T> factory, List<MetaField> projectionFields2, Map<MetaField, String> fieldParameterMap2, Map<String, MetaField<?>> parameterFieldMap2) {
		this.factory = factory;
		this.projectionFields = projectionFields2;
		this.fieldParameterMap = fieldParameterMap2;
		this.parameterFieldMap = parameterFieldMap2;
	}
	
	public MetaField<?> getMetaFieldByParameter(String parameter){
		return parameterFieldMap.get(parameter);
	}

	
	

	public SpiIndexQuery<T> createSpiAdapter(String indexName) {
		return factory.createQuery(indexName);
	}
	
	
}
