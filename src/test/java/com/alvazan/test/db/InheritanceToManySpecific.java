package com.alvazan.test.db;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.base.CursorToMany;
import com.alvazan.orm.api.base.CursorToManyImpl;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlManyToMany;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;

@NoSqlEntity
public class InheritanceToManySpecific {

	@NoSqlId
	private String id;
	
	@NoSqlManyToMany
	private List<InheritanceSub1> inheritance = new ArrayList<InheritanceSub1>();

	@NoSqlOneToMany
	private CursorToMany<InheritanceSub1> anotherSet = new CursorToManyImpl<InheritanceSub1>();
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<InheritanceSub1> getInheritance() {
		return inheritance;
	}

	public void addSomething(InheritanceSub1 sub) {
		inheritance.add(sub);
	}
}
