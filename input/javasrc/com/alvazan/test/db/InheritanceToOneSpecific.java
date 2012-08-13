package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.ManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlEntity;

@NoSqlEntity
public class InheritanceToOneSpecific {

	@Id
	private String id;
	
	@ManyToOne
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
