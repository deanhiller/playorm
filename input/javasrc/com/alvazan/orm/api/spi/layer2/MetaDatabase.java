package com.alvazan.orm.api.spi.layer2;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;

@Singleton
public class MetaDatabase {

	private Map<String, MetaTableDbo> colFamilyToMeta = new HashMap<String, MetaTableDbo>();
	
	public void addMetaClassDbo(MetaTableDbo metaClass) {
		colFamilyToMeta.put(metaClass.getTableName(), metaClass);
	}
	
	public MetaTableDbo getMeta(String tableName) {
		return colFamilyToMeta.get(tableName);
	}

}
