package com.alvazan.orm.api.spi2;

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlInheritance;
import com.alvazan.orm.api.base.anno.NoSqlInheritanceType;

@NoSqlEntity
@NoSqlInheritance(columnfamily={DboColumnCommonMeta.class, DboColumnToOneMeta.class, DboColumnToManyMeta.class},
		strategy=NoSqlInheritanceType.SINGLE_TABLE, discriminatorColumnName="classType")
public class DboAbstractColumnMeta {

	@Id
	private String id;
	
	private String columnName;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
}
