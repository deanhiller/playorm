package com.alvazan.orm.layer3.spi.index.inmemory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi.index.IndexReaderWriter;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;

public class SpiIndexQueryImpl implements SpiQueryAdapter {

	private static final Logger log = LoggerFactory.getLogger(SpiIndexQueryImpl.class);
	private IndexItems index;
	private String indexName;
	private Map<String, Object> parameterValues = new HashMap<String, Object>();
	private QueryFactory spiQuery;
	
	public void setup(IndexItems index, QueryFactory spiQuery) {
		this.index = index;
		this.spiQuery = spiQuery;
	}
	
	@Override
	public void setParameter(String parameterName, String value) {
		log.info("set param for index="+indexName+"  "+ parameterName +"="+value);
		parameterValues.put(parameterName, value);
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
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List getResultListImpl() throws ParseException, IOException {
		IndexReader reader = null;
		IndexSearcher searcher = null;
		try {
			List listOfPrimaryKeys = new ArrayList();
			//query the ram directory.  do we need to synchronize on the MetaClass
			//as any query on MetaClass can query this one index.  do not synchronize
			//on this class because there is one instace for each active query on
			//the same thread(some queries may be the same)
			
			Query q = this.spiQuery.getQuery(parameterValues);
		    reader = IndexReader.open(index.getRamDirectory());
			searcher = new IndexSearcher(reader);
			
			TopScoreDocCollector collector = TopScoreDocCollector.create(500, false);
			//need to insert a collector here...
		    searcher.search(q, collector);
		    
		    int total = collector.getTotalHits();
		    log.info("total results="+total);
		    
		    TopDocs docs = collector.topDocs();
		    
		    ScoreDoc[] scoreDocs = docs.scoreDocs;
		    for(ScoreDoc scoreDoc : scoreDocs) {
		    	Document doc = searcher.doc(scoreDoc.doc);
		    	String id = doc.get(IndexReaderWriter.IDKEY);
		    	listOfPrimaryKeys.add(id);
		    }
		    
			return listOfPrimaryKeys;
		} finally {
			if(reader != null)
				reader.close();
			if(searcher != null)
				searcher.close();
		}
	}

}
