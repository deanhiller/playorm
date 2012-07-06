package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.RAMDirectory;

public class Indice {
	
	private Map<String, RAMDirectory> nameToIndex = new ConcurrentHashMap<String, RAMDirectory>();

	public RAMDirectory findOrCreate(String indexName) {
		//synchronize on the name so we are not creating this twice on accident
		//if you don't intern then "hel"+"lo" != "hello" which could be very bad per 
		//Java Language Spec
		synchronized(indexName.intern()) {
			RAMDirectory ramDirectory = nameToIndex.get(indexName);
			if(ramDirectory == null) {
				ramDirectory = new RAMDirectory();
				nameToIndex.put(indexName, ramDirectory);
			}
			return ramDirectory;
		}
	}

	public RAMDirectory find(String indexName) {
		return findOrCreate(indexName);
		//we should throw exception if not exist later...???
//		RAMDirectory directory = nameToIndex.get(indexName);
//		if(directory == null)
//			throw new IllegalArgumentException("indexName="+indexName+" does NOT exist at this time.  You must add at least one record to the index before querying it");
//		return directory;
	}
}
