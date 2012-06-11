package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.impl.meta.MetaClass;
import com.alvazan.orm.impl.meta.MetaField;
import com.alvazan.orm.parser.NoSqlTreeParser;
import com.alvazan.orm.parser.QueryContext;

//I doubt we should set up query info for each different SPI 
//as we are using JPQL as language. The query info should be same to each different SPI
//The difference is how the SPI execute the query.
public class SpiQueryInfo {

	private QueryContext context ;
	
	private List<MetaField> projectionFields = new ArrayList<MetaField>();
	
	public void setup(MetaClass metaClass, String query) {
		//TODO: parse and setup this query once here to be used by ALL of the SpiIndexQuery objects.
		//NOTE: This is meta data to be re-used by all threads and all instances of query objects only!!!!
		context =NoSqlTreeParser.parse(query);
		
		
	}

}
