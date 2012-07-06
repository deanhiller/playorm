package com.alvazan.orm.impl.meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.antlr.runtime.tree.CommonTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.anno.NoSqlQuery;
import com.alvazan.orm.layer3.spi.index.IndexReaderWriter;
import com.alvazan.orm.layer3.spi.index.SpiIndexQueryFactory;
import com.alvazan.orm.parser.NoSqlTreeParser;
import com.alvazan.orm.parser.QueryContext;
import com.alvazan.orm.parser.tree.Attribute;
import com.alvazan.orm.parser.tree.FilterParameter;

public class ScannerForQuery {
	
	private static final Logger log = LoggerFactory.getLogger(ScannerForQuery.class);
	@Inject
	private IndexReaderWriter indexes;
	
	@SuppressWarnings("rawtypes")
	@Inject
	private Provider<MetaQuery> metaQueryFactory;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void createQueryAndAdd(MetaClass classMeta, NoSqlQuery query) {
		//This is a bit messed up and need to clean up one more step
		MetaQuery metaQuery = setup(classMeta, query.query());

		//we do lazy verify.
		classMeta.addQuery(query.name(), metaQuery);
	}	
	
	public <T> MetaQuery<T> setup(MetaClass<T> metaClass, String query) {
		//parse and setup this query once here to be used by ALL of the SpiIndexQuery objects.
		//NOTE: This is meta data to be re-used by all threads and all instances of query objects only!!!!
		MetaQuery<T> metaQuery = metaQueryFactory.get();
		
		CommonTree theTree = NoSqlTreeParser.parseTree(query);
		//We must walk the tree allowing 2 visitors to see it.
		//The first visitor would be ourselves maybe? to get all parameter info
		//The second visitor is the SPI Index so it can create it's "prototype" query (prototype pattern)
		
		QueryContext context = NoSqlTreeParser.parse(query);
		setupProjectionsAndAttributes(context,metaClass, metaQuery);
		
		return metaQuery;
	}
	
	@SuppressWarnings("unchecked")
	private <T> void setupProjectionsAndAttributes(QueryContext context, MetaClass<T> metaClass, MetaQuery<T> query) {
		List<MetaField> projectionFields = new ArrayList<MetaField>();
		Map<MetaField, String> fieldParameterMap = new HashMap<MetaField, String>();
		Map<String,MetaField<?>> parameterFieldMap = new HashMap<String, MetaField<?>>();
		
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
		
		SpiIndexQueryFactory factory = indexes.createQueryFactory();
		factory.setup(metaClass, query);
		
		query.setup(factory, projectionFields, fieldParameterMap, parameterFieldMap);
	}
	
}
