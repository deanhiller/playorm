package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.List;

import org.apache.lucene.store.RAMDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.impl.meta.MetaClass;
import com.alvazan.orm.impl.meta.MetaQuery;
import com.alvazan.orm.layer3.spi.index.SpiIndexQuery;

public class SpiIndexQueryImpl<T> implements SpiIndexQuery<T> {

	private static final Logger log = LoggerFactory.getLogger(SpiIndexQueryImpl.class);
	private RAMDirectory ramDir;
	private String indexName;

	@Override
	public void setParameter(String parameterName, String value) {
		log.info("set param for index="+indexName+"  "+ parameterName +"="+value);
	}

	@Override
	public List<T> getResultList() {
		//query the ram directory.  do we need to synchronize on the MetaClass
		//as any query on MetaClass can query this one index.  do not synchronize
		//on this class because there is one instace for each active query on
		//the same thread(some queries may be the same)
		
		
		return null;
	}

	public void setup(MetaClass<T> clazz, MetaQuery<T> info, RAMDirectory ramDir, String indexName) {
		this.ramDir = ramDir;
		this.indexName = indexName;
	}

}
