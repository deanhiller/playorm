package com.alvazan.orm.impl.meta.data;

import java.util.Comparator;

@SuppressWarnings("rawtypes")
public class MetaComparator implements Comparator<MetaAbstractClass> {

	@Override
	public int compare(MetaAbstractClass o1, MetaAbstractClass o2) {
		return o1.getMetaClass().getSimpleName().compareToIgnoreCase(o2.getMetaClass().getSimpleName());
	}

}
