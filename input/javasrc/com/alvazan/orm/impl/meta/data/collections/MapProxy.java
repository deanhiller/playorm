package com.alvazan.orm.impl.meta.data.collections;

import java.util.HashMap;
import java.util.List;

import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.data.MetaClass;

public class MapProxy<K, V> extends HashMap<K, V> {

	private static final long serialVersionUID = 1L;
	
	public MapProxy(NoSqlSession session, MetaClass<?> classMeta,
			List<byte[]> keys) {
	}

}
