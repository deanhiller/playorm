package com.alvazan.orm.layer3.spi.index.inmemory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

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
import com.alvazan.orm.api.spi.index.exc.IndexNotYetExistException;

public class SpiIndexQueryImpl implements SpiQueryAdapter {

	private static final Logger log = LoggerFactory.getLogger(SpiIndexQueryImpl.class);
	@Inject
	private Indice indice;
	private String indexName;
	private Map<String, Object> parameterValues = new HashMap<String, Object>();
	private SpiMetaQueryImpl spiQuery;
	
	public void setup(String indexName, SpiMetaQueryImpl spiQuery) {
		this.indexName = indexName;
		this.spiQuery = spiQuery;
	}
	
	@Override
	public void setParameter(String parameterName, Object value) {
		log.info("set param for query "+ parameterName +"="+value);
		parameterValues.put(parameterName, value);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List getResultList() {
			try {
				return getResultListImpl();
			} catch (ParseException e) {
				throw new RuntimeException("bug, we setup parsing wrong", e);
			} catch (IOException e) {
				throw new RuntimeException("some kind of index failure",e);
			}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List getResultListImpl() throws ParseException, IOException {
		IndexReader reader = null;
		IndexSearcher searcher = null;
		try {
			IndexItems items = indice.find(indexName);
			if(items == null)
				throw new IndexNotYetExistException("Perhaps, you forgot to call flush?  You must save at least ONE entity to the index" +
						" for it to be created AND call entityManager.flush to flush the index changes.  indexname="+indexName);
				
			List listOfPrimaryKeys = new ArrayList();
			//query the ram directory.  do we need to synchronize on the MetaClass
			//as any query on MetaClass can query this one index.  do not synchronize
			//on this class because there is one instace for each active query on
			//the same thread(some queries may be the same)
			
			Query q = this.spiQuery.getQuery(parameterValues);
		    reader = IndexReader.open(items.getRamDirectory());
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
