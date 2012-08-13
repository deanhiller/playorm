package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlInheritance;
import com.alvazan.orm.api.base.anno.NoSqlInheritanceType;

@NoSqlEntity
@NoSqlInheritance(subclassesToScan={InheritanceSub1.class, InheritanceSub2.class},
		strategy=NoSqlInheritanceType.SINGLE_TABLE, discriminatorColumnName="classType")
public class InheritanceSuper {

	@NoSqlId
	private String id;
	
	private long num;

	private String lastName;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getNum() {
		return num;
	}

	public void setNum(long num) {
		this.num = num;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	
}
