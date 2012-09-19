package com.alvazan.orm.api.base;

import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class NoSqlDao {

	public static Cursor<KeyValue<DboTableMeta>> findAllTables(NoSqlEntityManager mgr) {
		Query<DboTableMeta> query = mgr.createNamedQuery(DboTableMeta.class, "findAll");
		return query.getResults();
	}
	
	public static Cursor<KeyValue<DboTableMeta>> findTablesWithPrefix(NoSqlEntityManager mgr, String prefix) {
		Query<DboTableMeta> query = mgr.createNamedQuery(DboTableMeta.class, "findLike");
		query.setParameter("prefix", prefix);
		//Just tack on ONE letter that is definitely LARGER than any other character they can supply
		//and we will be able to get all results with that prefix..
		String lastValue = prefix+"\uffff";
		query.setParameter("modifiedPrefix", lastValue);
		return query.getResults();
	}
}
