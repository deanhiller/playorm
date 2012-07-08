package com.alvazan.orm.impl.meta.query;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;

@Singleton
public class MetaInfoMap {

	private Map<String, MetaClassDbo> colFamilyToMeta = new HashMap<String, MetaClassDbo>();
	
	public void addMetaClassDbo(MetaClassDbo metaClass) {
		colFamilyToMeta.put(metaClass.getTableName(), metaClass);
	}
	
	public MetaClassDbo getMeta(String tableName) {
		return colFamilyToMeta.get(tableName);
	}

}
