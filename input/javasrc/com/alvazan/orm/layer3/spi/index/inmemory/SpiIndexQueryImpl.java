package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.List;

import com.alvazan.orm.layer3.spi.index.SpiIndexQuery;

public class SpiIndexQueryImpl implements SpiIndexQuery {

	private SpiQueryInfo info;

	@Override
	public void setParameter(String name, Object value) {
	}

	@Override
	public List getResultList() {
		
		return null;
	}

	public void setInfo(SpiQueryInfo info) {
		this.info = info;
	}
}
