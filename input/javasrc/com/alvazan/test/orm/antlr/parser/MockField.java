package com.alvazan.test.orm.antlr.parser;

import com.alvazan.orm.impl.meta.data.MetaQueryFieldInfo;

public class MockField implements MetaQueryFieldInfo {

	@SuppressWarnings("rawtypes")
	@Override
	public Class getFieldType() {
		return null;
	}

	@Override
	public String translateIfEntity(Object value) {
		return null;
	}

}
