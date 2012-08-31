package com.alvazan.orm.impl.meta.data;

import java.util.List;

import com.alvazan.orm.api.z3api.meta.MetaQuery;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;




public interface MetaClass<T> {

	Class<T> getMetaClass();

	String getColumnFamily();

	MetaIdField<T> getIdField();

	boolean hasIndexedField();

	MetaQuery<T> getNamedQuery(String name);

	KeyValue<T> translateFromRow(Row row, NoSqlSession session);

	RowToPersist translateToRow(T entity);

	Object fetchId(T entity);

	byte[] convertIdToNoSql(Object pk);

	List<IndexData> findIndexRemoves(NoSqlProxy proxy, byte[] rowKey);

	MetaField<T> getMetaFieldByCol(String columnName);

	boolean isPartitioned();


}
