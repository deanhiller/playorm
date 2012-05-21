package com.alvazan.orm.impl.meta;

import com.alvazan.orm.layer3.spi.index.SpiIndexQueryFactory;

public class MetaQuery<T> {

	private SpiIndexQueryFactory factory;

	public void setFactory(SpiIndexQueryFactory factory) {
		this.factory = factory;
	}

	public SpiIndexQueryFactory getFactory() {
		return factory;
	}
	
}
