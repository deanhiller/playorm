package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;

@NoSqlEntity
public class InheritanceToOneSpecific {

	@NoSqlId
	private String id;
	
	@NoSqlManyToOne
	private InheritanceSub1 inheritance;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public InheritanceSub1 getInheritance() {
		return inheritance;
	}

	public void setInheritance(InheritanceSub1 nameToEntity) {
		this.inheritance = nameToEntity;
	}

}
