package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Singleton;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

@Singleton
public class Indice {
	
	private static StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
	private Map<String, IndexItems> nameToIndex = new ConcurrentHashMap<String, IndexItems>();

	public IndexItems findOrCreate(String indexName) {
		//synchronize on the name so we are not creating this twice on accident
		//if you don't intern then "hel"+"lo" != "hello" which could be very bad per 
		//Java Language Spec
		synchronized(indexName.intern()) {
			IndexItems items = nameToIndex.get(indexName);
			if(items == null) {
				RAMDirectory ramDirectory = new RAMDirectory();
				items = new IndexItems(analyzer, ramDirectory, indexName);
				nameToIndex.put(indexName, items);
			}
			return items;
		}
	}

	public IndexItems find(String indexName) {
		return nameToIndex.get(indexName);
	}

	public void clear() {
		nameToIndex.clear();
	}
	
}
