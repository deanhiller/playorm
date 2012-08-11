package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.ManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlEntity;

@NoSqlEntity
public class InheritanceToOne {

	@Id
	private String id;
	
	@ManyToOne
	private InheritanceSuper inheritance;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public InheritanceSuper getInheritance() {
		return inheritance;
	}

	public void setInheritance(InheritanceSuper nameToEntity) {
		this.inheritance = nameToEntity;
	}

}
