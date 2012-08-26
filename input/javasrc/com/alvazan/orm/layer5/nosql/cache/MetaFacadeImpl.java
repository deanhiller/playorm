package com.alvazan.orm.layer5.nosql.cache;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;

public class MetaFacadeImpl implements MetaFacade {

	private NoSqlEntityManager mgr;
	private DboDatabaseMeta metaInfo;
	
	public MetaFacadeImpl(NoSqlEntityManager mgr, DboDatabaseMeta metaInfo2) {
		this.mgr = mgr;
		this.metaInfo = metaInfo2;
	}

	@Override
	public DboTableMeta getColumnFamily(String tableName) {
		DboTableMeta metaClass = metaInfo.getMeta(tableName);
		if(metaClass == null && mgr != null)
			metaClass = mgr.find(DboTableMeta.class, tableName);
		return metaClass;
	}

	@Override
	public DboColumnMeta getColumnMeta(DboTableMeta metaClass, String columnName) {
		return metaClass.getColumnMeta(columnName);
	}

}
