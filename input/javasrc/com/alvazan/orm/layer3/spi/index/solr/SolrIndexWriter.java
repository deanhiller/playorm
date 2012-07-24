package com.alvazan.orm.layer3.spi.index.solr;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.spi.index.IndexAdd;
import com.alvazan.orm.api.spi.index.IndexReaderWriter;
import com.alvazan.orm.api.spi.index.IndexRemove;
import com.alvazan.orm.api.spi.index.SpiMetaQuery;

public class SolrIndexWriter implements IndexReaderWriter {

	@Inject
	private Provider<SolrMetaQueryImpl> factory;
	
	@Override
	public void sendRemoves(
			Map<String, List<? extends IndexRemove>> removeFromIndex) {
	}

	@Override
	public void sendAdds(Map<String, List<IndexAdd>> addToIndex) {
	}

	@Override
	public SpiMetaQuery createQueryFactory() {
		return factory.get();
	}

	@Override
	public void clearIndexesIfInMemoryType() {
		//throw new UnsupportedOperationException("Not supported by actual index implementations.  Only can be used with in-memory index.");
	}





}
