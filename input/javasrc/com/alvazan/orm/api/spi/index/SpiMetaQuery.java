package com.alvazan.orm.api.spi.index;

import java.util.Map;

import org.apache.lucene.search.Query;

import com.alvazan.orm.impl.meta.query.MetaFieldDbo;



public interface SpiMetaQuery {
	
	/**
	 * We will call this method EVERY time we want to run a query so that the SpiIndexQuery can have
	 * state and store parameters!!!
	 * 
	 * NOTE: SpiIndexQuery typically has a prototype pattern where it has the query that will just
	 * be cloned as the adapter is created so the adapter can use it, and so one indexQuery can 
	 * have many SpiQueryAdapters associated with it that came from the same query but threads supply
	 * different parameters to it.  
	 * 
	 * @param indexName
	 * @return A BRAND NEW instance of SpiQueryAdapter(must not be one you have returned previously)
	 */
	public SpiQueryAdapter createQueryInstanceFromQuery(String indexName);


	public void onHyphen(int type);


	public void onComparator(String parameter, MetaFieldDbo attributeField,
			int type);
	
	
	public Query getQuery(Map<String, Object> parameterValues);
	
}
