package com.alvazan.orm.impl.meta;

import java.util.HashMap;
import java.util.Map;

public class InfoForWiring {

	private MetaQueryClassInfo noAliasTable;
	private Map<String, MetaQueryClassInfo> aliasToMeta = new HashMap<String, MetaQueryClassInfo>();
	private boolean selectStarDefined;
	
	public void setNoAliasTable(MetaQueryClassInfo metaClass) {
		this.noAliasTable = metaClass;
	}

	public MetaQueryClassInfo getNoAliasTable() {
		return noAliasTable;
	}

	public void put(String alias, MetaQueryClassInfo metaClass) {
		aliasToMeta.put(alias, metaClass);
	}

	public MetaQueryClassInfo getInfoFromAlias(String alias) {
		return aliasToMeta.get(alias);
	}

	public void setSelectStarDefined(boolean defined) {
		selectStarDefined = defined;
	}

	public boolean isSelectStarDefined() {
		return selectStarDefined;
	}
	

}
