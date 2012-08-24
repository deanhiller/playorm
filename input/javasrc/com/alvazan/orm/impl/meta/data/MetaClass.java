package com.alvazan.orm.impl.meta.data;

import java.util.List;

import com.alvazan.orm.api.spi3.meta.IndexData;
import com.alvazan.orm.api.spi3.meta.MetaQuery;
import com.alvazan.orm.api.spi3.meta.RowToPersist;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.Row;




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
