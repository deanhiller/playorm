package com.alvazan.orm.layer3.spi.index;

import org.antlr.runtime.tree.CommonTree;


public interface SpiMetaQuery<T> {
	
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
	public SpiQueryAdapter<T> createQueryInstanceFromQuery(String indexName);

	/**
	 * The root of this tree is AND, OR, EQ, NE, GT, etc. etc.  You should form your query by walking
	 * this tree at this point and save a prototype of that query so you can clone, clone, clone each
	 * time that createQueryInstanceFromQuery is called!!!  Then, later on SpiQueryAdapter, parameters
	 * will be injected into it which is why you must keep returning NEW SpiQueryAdapters every time
	 * or different threads will step on the instance.
	 * 
	 * @param expression
	 */
	public void formQueryFromAstTree(CommonTree expression);
	
}
