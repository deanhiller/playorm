package com.alvazan.orm.api.z8spi.iter;

import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.layer3.typed.IndiceCursorProxy;

public class IndiceToVirtual implements DirectCursor<byte[]> {

	private DboTableMeta meta;
	private DirectCursor<byte[]> noSqlKeys;

	public IndiceToVirtual(DboTableMeta meta, DirectCursor<byte[]> noSqlKeys) {
		this.meta = meta;
		this.noSqlKeys = noSqlKeys;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "IndiceToVirtual(keyToVirtualKeyTranslatorCursor)["+tabs+noSqlKeys+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	

	@Override
	public Holder<byte[]> nextImpl() {
		Holder<byte[]> nextHolder = noSqlKeys.nextImpl();
		if (nextHolder == null)
			return null;
		byte[] next = nextHolder.getValue();
		return new Holder<byte[]>(meta.getIdColumnMeta().formVirtRowKey(next));
	}

	@Override
	public Holder<byte[]> previousImpl() {
		Holder<byte[]> nextHolder = noSqlKeys.previousImpl();
		if (nextHolder == null)
			return null;
		byte[] next = nextHolder.getValue();
		return new Holder<byte[]>(meta.getIdColumnMeta().formVirtRowKey(next));
	}

	@Override
	public void beforeFirst() {
		noSqlKeys.beforeFirst();
		
	}

	@Override
	public void afterLast() {
		noSqlKeys.afterLast();
	}
}
