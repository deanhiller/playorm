package com.alvazan.orm.impl.meta.scan;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.impl.meta.query.MetaClassDbo;

public class InfoForWiring {

	private MetaClassDbo noAliasTable;
	private Map<String, MetaClassDbo> aliasToMeta = new HashMap<String, MetaClassDbo>();
	private boolean selectStarDefined;
	
	public void setNoAliasTable(MetaClassDbo metaClass) {
		this.noAliasTable = metaClass;
	}

	public MetaClassDbo getNoAliasTable() {
		return noAliasTable;
	}

	public void put(String alias, MetaClassDbo metaClass) {
		aliasToMeta.put(alias, metaClass);
	}

	public MetaClassDbo getInfoFromAlias(String alias) {
		return aliasToMeta.get(alias);
	}

	public void setSelectStarDefined(boolean defined) {
		selectStarDefined = defined;
	}

	public boolean isSelectStarDefined() {
		return selectStarDefined;
	}
	

}
