package com.alvazan.orm.layer3.typed;

import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;

public class TypedProxyWrappingCursor<T> implements DirectCursor<byte[]> {

	private DboColumnMeta idMeta;
	private DirectCursor<T> keys;
	
	public TypedProxyWrappingCursor(DboColumnMeta idMeta, DirectCursor<T> keys2) {
		Precondition.check(keys2, "keys");
		this.idMeta = idMeta;
		this.keys = keys2;
	}
	

	@Override
	public Holder<byte[]> nextImpl() {
		Holder<T> nextKey = keys.nextImpl();
		if (nextKey == null)
			return null;
		T k = nextKey.getValue();
		byte[] rowK = idMeta.convertToStorage2(k);
		return new Holder<byte[]>(rowK);
	}

	@Override
	public Holder<byte[]> previousImpl() {
		Holder<T> nextKey = keys.previousImpl();
		if (nextKey == null)
			return null;
		T k = nextKey.getValue();
		byte[] rowK = idMeta.convertToStorage2(k);
		return new Holder<byte[]>(rowK);
	}

	@Override
	public void beforeFirst() {
		keys.beforeFirst();
	}

	@Override
	public void afterLast() {
		keys.afterLast();
	}

}
