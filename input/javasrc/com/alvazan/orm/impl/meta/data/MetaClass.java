package com.alvazan.orm.impl.meta.data;

import java.util.List;

import com.alvazan.orm.api.spi2.IndexData;
import com.alvazan.orm.api.spi2.KeyValue;
import com.alvazan.orm.api.spi2.MetaQuery;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi2.RowToPersist;
import com.alvazan.orm.api.spi3.db.Row;




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




}
