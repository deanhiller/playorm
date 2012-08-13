package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlEntity;

@NoSqlEntity
public class InheritanceToOne {

	@NoSqlId
	private String id;
	
	//@ManyToOne
	//private ToOneProvider<InheritanceSuper> inheritance = new ToOneProvider<InheritanceSuper>();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

//	public InheritanceSuper getInheritance() {
//		return inheritance.get();
//	}
//
//	public void setInheritance(InheritanceSuper nameToEntity) {
//		inheritance.set(nameToEntity);
//	}

}
