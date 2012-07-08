package com.alvazan.test;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLucene {
	private static final Logger log = LoggerFactory.getLogger(TestLucene.class);
	@Test
	public void test() throws Exception{    
		// 0. Specify the analyzer for tokenizing text.
    //    The same analyzer should be used for indexing and searching
    StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);

    // 1. create the index
    Directory index = new RAMDirectory();

    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_35, analyzer);

    IndexWriter w = new IndexWriter(index, config);
    addDoc(w, "Lucene in Action","title","Managing Gigabytes","body");
    addDoc(w, "Lucene for Dummies","title","The Art of Computer Science","body");
    addDoc(w, "Managing Gigabytes","body","TT for Dummies","title");
    addDoc(w, "The Art of Computer Science","body","TT for Dummies","title");
    w.close();

    //query (title = lucene or body =managing) or (title = lucene and body =art) 
    // 2. query
 

    // the "title" arg specifies the default field to use
    // when no field is explicitly specified in the query.
    BooleanQuery booleanQuery = new BooleanQuery();
    
    Query q = new QueryParser(Version.LUCENE_35, "title", analyzer).parse("lucene");
    Query q2 = new QueryParser(Version.LUCENE_35, "body", analyzer).parse("managing");
    
    BooleanQuery booleanQuery2 = new BooleanQuery();
    Query q3 = new QueryParser(Version.LUCENE_35, "title", analyzer).parse("lucene");
    Query q4 = new QueryParser(Version.LUCENE_35, "body", analyzer).parse("art");
    booleanQuery2.add(q3,BooleanClause.Occur.MUST);
    booleanQuery2.add(q4,BooleanClause.Occur.MUST);
    
    booleanQuery.add(q, BooleanClause.Occur.SHOULD);
    booleanQuery.add(q2, BooleanClause.Occur.SHOULD);
    
    BooleanQuery booleanQuery3 = new BooleanQuery();
    booleanQuery3.add(booleanQuery,BooleanClause.Occur.SHOULD);
    booleanQuery3.add(booleanQuery2,BooleanClause.Occur.SHOULD);
    
    // 3. search
    int hitsPerPage = 10;
    IndexReader reader = IndexReader.open(index);
    IndexSearcher searcher = new IndexSearcher(reader);
    TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
    searcher.search(booleanQuery3, collector);
    ScoreDoc[] hits = collector.topDocs().scoreDocs;
   
    // 4. display results
    log.info("Found " + hits.length + " hits.");
    for(int i=0;i<hits.length;++i) {
      int docId = hits[i].doc;
      Document d = searcher.doc(docId);
      log.info((i + 1) + ". " + d.get("title"));
    }

    // searcher can only be closed when there
    // is no need to access the documents any more.
    searcher.close();
  }

  private static void addDoc(IndexWriter w, String value,String field,String value2, String field2) throws IOException {
    Document doc = new Document();
    doc.add(new Field(field, value, Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field(field2, value2, Field.Store.YES, Field.Index.ANALYZED));
    w.addDocument(doc);
  }
}