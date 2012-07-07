package com.alvazan.orm.layer3.spi.index.inmemory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi.index.SpiQueryAdapter;

public class SpiIndexQueryImpl implements SpiQueryAdapter {

	private static final Logger log = LoggerFactory.getLogger(SpiIndexQueryImpl.class);
	private IndexItems index;
	private String indexName;
	//This is the whole query with a few pieces left blank(ie. the parameters that need to be filled in)
	private String[] piecesOfQuery;
	private Map<String, List<Integer>> paramNameToListOfIndex;
	
	public void setup(IndexItems index, String[] piecesOfQuery, Map<String, List<Integer>> paramNameToListOfIndex) {
		this.index = index;
		this.piecesOfQuery = piecesOfQuery;
		this.paramNameToListOfIndex = paramNameToListOfIndex;
	}
	
	@Override
	public void setParameter(String parameterName, String value) {
		log.info("set param for index="+indexName+"  "+ parameterName +"="+value);
		List<Integer> list = paramNameToListOfIndex.get(parameterName);
		for(Integer index : list) {
			piecesOfQuery[index] = value;
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List getResultList() {
		try {
			return getResultListImpl();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public List getResultListImpl() throws ParseException, IOException {
		IndexReader reader = null;
		IndexSearcher searcher = null;
		try {
			List listOfPrimaryKeys = new ArrayList();
			//query the ram directory.  do we need to synchronize on the MetaClass
			//as any query on MetaClass can query this one index.  do not synchronize
			//on this class because there is one instace for each active query on
			//the same thread(some queries may be the same)
			String querystr = "";
			for(String piece : piecesOfQuery) {
				querystr += piece;
			}
			
			Query q = new QueryParser(Version.LUCENE_35, "title", index.getAnalyzer()).parse(querystr);
			
		    reader = IndexReader.open(index.getRamDirectory());
			searcher = new IndexSearcher(reader);
			//need to insert a collector here...
		    searcher.search(q, null);
		    
			return listOfPrimaryKeys;
		} finally {
			if(reader != null)
				reader.close();
			if(searcher != null)
				searcher.close();
		}
	}

}
