package com.alvazan.test.db;

import java.util.List;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;

@NoSqlDiscriminatorColumn(value="sub1")
@NoSqlQueries({
   @NoSqlQuery(name="findAll", query="select s from TABLE as s"),
   @NoSqlQuery(name="findByName", query="select s from TABLE as s where s.name = :name")
})
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

	public static InheritanceSub1 findByName(NoSqlEntityManager mgr, String name) {
		Query<InheritanceSub1> query = mgr.createNamedQuery(InheritanceSub1.class, "findByName");
		query.setParameter("name", name);
		return query.getSingleObject();
	}
}
