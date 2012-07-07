package com.alvazan.test.orm.antlr.parser;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.impl.meta.MetaQueryClassInfo;
import com.alvazan.orm.impl.meta.MetaQueryFieldInfo;

public class MockMeta implements MetaQueryClassInfo {

	private Map<String, MetaQueryFieldInfo> nameToField = new HashMap<String, MetaQueryFieldInfo>();
	
	public MockMeta(Map<String, MetaQueryFieldInfo> map) {
		this.nameToField = map;
	}
	
	@Override
	public Collection<? extends MetaQueryFieldInfo> getMetaFields() {
		return nameToField.values();
	}

	@Override
	public MetaQueryFieldInfo getMetaField(String attributeName) {
		return nameToField.get(attributeName);
	}

	@Override
	public String getIdFieldName() {
		return "id";
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class getMetaClass() {
		// TODO Auto-generated method stub
		return null;
	}


}
