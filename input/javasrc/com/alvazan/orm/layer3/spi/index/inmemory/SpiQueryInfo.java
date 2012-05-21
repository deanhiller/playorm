package com.alvazan.orm.layer3.spi.index.inmemory;

public class SpiQueryInfo {

	public void setup(String query) {
		//TODO: parse and setup this query once here to be used by ALL of the SpiIndexQuery objects.
		//NOTE: This is meta data to be re-used by all threads and all instances of query objects only!!!!
	}

}
