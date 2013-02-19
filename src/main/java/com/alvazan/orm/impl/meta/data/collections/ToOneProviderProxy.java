package com.alvazan.orm.impl.meta.data.collections;

import com.alvazan.orm.api.base.ToOneProvider;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.impl.meta.data.MetaAbstractClass;
import com.alvazan.orm.impl.meta.data.Tuple;

public class ToOneProviderProxy<T> extends ToOneProvider<T> {

	private MetaAbstractClass<T> classMeta;
	private byte[] value;
	private NoSqlSession session;

	public ToOneProviderProxy(MetaAbstractClass<T> classMeta, byte[] value,
			NoSqlSession session) {
		this.classMeta = classMeta;
		this.value = value;
		this.session = session;
	}

	@Override
	public T get() {
		if(inst == null) {
			byte[] virtualKey = classMeta.getIdField().getMetaIdDbo().formVirtRowKey(value);
			Row row = session.find(classMeta.getMetaDbo(), virtualKey);
			if(row == null)
				throw new RuntimeException("corrupt databuse? as we could not find the row corresponding this this key");
			Tuple<T> tuple = classMeta.convertIdToProxy(row, session, value, null);
			inst = tuple.getProxy();
		}
		return inst;
	}

}
