package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.RAMDirectory;

public class Indice {
	
	private Map<String, RAMDirectory> nameToIndex = new ConcurrentHashMap<String, RAMDirectory>();

	public RAMDirectory findOrCreate(String indexName) {
		//synchronize on the name so we are not creating this twice on accident
		synchronized(indexName.intern()) {
			RAMDirectory ramDirectory = nameToIndex.get(indexName);
			if(ramDirectory == null) {
				ramDirectory = new RAMDirectory();
				nameToIndex.put(indexName, ramDirectory);
			}
			return ramDirectory;
		}
	}	
}
