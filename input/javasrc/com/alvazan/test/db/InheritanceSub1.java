package com.alvazan.test.db;

import java.util.List;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlQuery;

@NoSqlDiscriminatorColumn(value="sub1")
@NoSqlQuery(name="findAll", query="select s from TABLE as s")
public class InheritanceSub1 extends InheritanceSuper {

	@NoSqlIndexed
	private String name;
	
	private String diff;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDiff() {
		return diff;
	}

	public void setDiff(String diff) {
		this.diff = diff;
	}
	
	public static List<InheritanceSub1> findAll(NoSqlEntityManager mgr) {
		Query<InheritanceSub1> query = mgr.createNamedQuery(InheritanceSub1.class, "findAll");
		return query.getResultList(0, 50);
	}
}
