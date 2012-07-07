package com.alvazan.orm.layer3.spi.index.inmemory;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.RAMDirectory;

public class IndexItems {

	private StandardAnalyzer analyzer;
	private RAMDirectory ramDirectory;
	private String indexName;

	public IndexItems(StandardAnalyzer analyzer, RAMDirectory ramDirectory, String indexName) {
		this.analyzer = analyzer;
		this.ramDirectory = ramDirectory;
		this.indexName = indexName;
	}

	public StandardAnalyzer getAnalyzer() {
		return analyzer;
	}

	public RAMDirectory getRamDirectory() {
		return ramDirectory;
	}

	public String getIndexName() {
		return indexName;
	}

}
