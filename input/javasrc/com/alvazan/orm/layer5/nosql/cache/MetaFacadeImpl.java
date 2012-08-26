package com.alvazan.orm.layer5.nosql.cache;

import com.alvazan.orm.api.base.NoSqlEntityManager;

public class MetaFacadeImpl implements MetaFacade {

	private NoSqlEntityManager mgr;

	public MetaFacadeImpl(NoSqlEntityManager mgr) {
		this.mgr = mgr;
	}

}
