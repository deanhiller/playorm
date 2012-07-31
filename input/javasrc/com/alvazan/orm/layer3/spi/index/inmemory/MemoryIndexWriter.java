package com.alvazan.orm.layer3.spi.index.inmemory;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.spi3.index.IndexReaderWriter;
import com.alvazan.orm.api.spi3.index.SpiMetaQuery;

public class MemoryIndexWriter implements IndexReaderWriter {

	@Inject
	private Provider<SpiMetaQueryImpl> factory;
	@Override
	public SpiMetaQuery createQueryFactory() {
		return factory.get();
	}

}
