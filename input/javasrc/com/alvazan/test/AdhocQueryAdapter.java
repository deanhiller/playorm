package com.alvazan.test;

import java.util.List;

import com.alvazan.orm.api.spi.db.Row;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.data.MetaQuery;

@SuppressWarnings("rawtypes")
public class AdhocQueryAdapter {

	private SpiQueryAdapter indexQuery;
	private NoSqlSession session;
	private MetaQuery metaQuery;
	
	public void setup(MetaQuery metaQuery, SpiQueryAdapter spiQueryAdapter) {
		this.metaQuery = metaQuery;
		this.indexQuery = spiQueryAdapter;
	}

	@SuppressWarnings("unchecked")
	public List<Row> getResultList() {
		List primaryKeys = indexQuery.getResultList();
		String colFamily = metaQuery.getTargetTable().getTableName();
		return  session.find(colFamily, primaryKeys);
	}
	
}
