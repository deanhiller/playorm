package com.alvazan.orm.layer3.spi.index.solr;

import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.spi.index.IndexAdd;
import com.alvazan.orm.api.spi.index.IndexReaderWriter;
import com.alvazan.orm.api.spi.index.IndexRemove;
import com.alvazan.orm.api.spi.index.SpiMetaQuery;

public class SolrIndexWriter implements IndexReaderWriter {

	@Override
	public void sendRemoves(
			Map<String, List<? extends IndexRemove>> removeFromIndex) {
	}

	@Override
	public void sendAdds(Map<String, List<IndexAdd>> addToIndex) {
	}

	@Override
	public SpiMetaQuery createQueryFactory() {
		// TODO Auto-generated method stub
		return null;
	}





}
