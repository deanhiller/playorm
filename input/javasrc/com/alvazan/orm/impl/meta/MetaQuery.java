package com.alvazan.orm.impl.meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.layer3.spi.index.SpiIndexQuery;
import com.alvazan.orm.layer3.spi.index.SpiIndexQueryFactory;
import com.alvazan.orm.parser.NoSqlTreeParser;
import com.alvazan.orm.parser.QueryContext;
import com.alvazan.orm.parser.tree.Attribute;
import com.alvazan.orm.parser.tree.FilterParameter;

@SuppressWarnings("rawtypes")
public class MetaQuery<T> {

	private static final Logger log = LoggerFactory.getLogger(MetaQuery.class);
	private QueryContext context ;
	
	private List<MetaField> projectionFields = new ArrayList<MetaField>();
	
	private Map<MetaField, String> fieldParameterMap = new HashMap<MetaField, String>();
	
	private Map<String,MetaField<?>> parameterFieldMap = new HashMap<String, MetaField<?>>();
	private SpiIndexQueryFactory<T> factory;
	
	public void setup(MetaClass<T> metaClass, String query, SpiIndexQueryFactory<T> factory) {
		//TODO: parse and setup this query once here to be used by ALL of the SpiIndexQuery objects.
		//NOTE: This is meta data to be re-used by all threads and all instances of query objects only!!!!
		context =NoSqlTreeParser.parse(query);
		setupProjectionsAndAttributes(context,metaClass);
		factory.setup(metaClass, this);
		this.factory = factory;
	}
	
	
	public MetaField<?> getMetaFieldByParameter(String parameter){
		return parameterFieldMap.get(parameter);
	}

	
	@SuppressWarnings("unchecked")
	private void setupProjectionsAndAttributes(QueryContext context,MetaClass metaClass) {
		List<Attribute> projections = context.getSelectClause().getProjections();
		
		for(Attribute projection:projections){
			if(projection.getAttributeName().equals("*")){
				projectionFields.addAll(metaClass.getMetaFields());
			}else{
				MetaField projectionField = metaClass.getMetaField(projection.getAttributeName());
				if(projectionField==null)
					throw new IllegalArgumentException("There is no " + projection
							+ " exists for class " + metaClass);
 				projectionFields.add(projectionField);
			}
			
		}
		//there might be no filter at all
		if(context.getWhereClause()==null)
			return;
		
		Map<Attribute, FilterParameter> parameterMap = context.getWhereClause()
				.getParameterMap();
		for (Attribute attribute : parameterMap.keySet()) {
			MetaField<?> attributeField = metaClass.getMetaField(attribute
					.getAttributeName());
			if (attributeField == null) {
				String idFieldName = metaClass.getIdField().getField()
						.getName();
				if (idFieldName.equals(attribute.getAttributeName())) {
					// TODO id field
					log.info("Well, id encountered");
				} else
					throw new IllegalArgumentException("There is no "
							+ attribute.getAttributeName()
							+ " exists for class " + metaClass);
			}

			String parameter = parameterMap.get(attribute).getParameter();
			fieldParameterMap.put(attributeField, parameter);
			parameterFieldMap.put(parameter, attributeField);
		}
	}


	public SpiIndexQuery<T> createSpiAdapter(String indexName) {
		return factory.createQuery(indexName);
	}
	
	
}
