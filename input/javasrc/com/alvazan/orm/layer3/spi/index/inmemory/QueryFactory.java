package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.antlr.runtime.tree.CommonTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.layer3.spi.index.SpiMetaQuery;
import com.alvazan.orm.layer3.spi.index.SpiQueryAdapter;

public class QueryFactory<T> implements SpiMetaQuery<T> {

	private static final Logger log = LoggerFactory.getLogger(QueryFactory.class);
	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private Indice indice;
	private String[] piecesOfQuery;
	private Map<String, List<Integer>> paramNameToIndexList;
	
	public void init(Indice indice) {
		this.indice = indice;
	}
	
	@Override
	public SpiQueryAdapter<T> createQueryInstanceFromQuery(String indexName) {
		log.info("creating query for index="+indexName);
		SpiIndexQueryImpl<T> indexQuery = factory.get();
		IndexItems index = indice.find(indexName);
		indexQuery.setup(index, piecesOfQuery, paramNameToIndexList);
		return indexQuery;
	}

	@Override
	public void formQueryFromAstTree(CommonTree expression) {
		//Now, we need to create all the pieces of the query!!!
		//One example of this might be
		//string[0] = [property:
		//string[1] = null (left empty as needs to be filled in...looked up in paramNameToIndexList
		//string[2] = ]
		walkTheTree(expression);
	}

	private void walkTheTree(CommonTree expression) {
		
	}
}
