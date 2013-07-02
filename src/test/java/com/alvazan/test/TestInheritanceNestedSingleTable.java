package com.alvazan.test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.test.db.InheritedNestedA;
import com.alvazan.test.db.InheritedNestedB;
import com.alvazan.test.db.InheritedNestedBaseForC;
import com.alvazan.test.db.InheritedNestedC;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestInheritanceNestedSingleTable {

	private static NoSqlEntityManagerFactory factory;
	private NoSqlEntityManager mgr;

	@BeforeClass
	public static void setup() {
		factory = FactorySingleton.createFactoryOnce();
	}
	
	@Before
	public void createEntityManager() {
		mgr = factory.createEntityManager();
	}
	@After
	public void clearDatabase() {
		NoSqlEntityManager other = factory.createEntityManager();
		other.clearDatabase(true);
	}
	
	@Test
	public void testSpecificQuery() {

        InheritedNestedA top1 = generate(10);
        InheritedNestedA top2 = generate(20);
        InheritedNestedA top3 = generate(30);

        persist(mgr, top1);

        mgr.flush();

            List<KeyValue<InheritedNestedA>> listOfA = mgr.findAllList(InheritedNestedA.class, Arrays.asList(10,20,30));
            for (KeyValue<InheritedNestedA> kvA : listOfA) {
                Object key = kvA.getKey();
                System.out.println("Got A for "+key);
                InheritedNestedA a = kvA.getValue();
                for (InheritedNestedB b : a.getSetOfB()) {
                    System.out.println("    with b#"+b.getIdForB());
                    for (InheritedNestedBaseForC cBase : b.getCs()) {
                        System.out.println("        with c#"+cBase.getIdForC());
                    }
                }
            }
	}

    private static void persist(NoSqlEntityManager mgr, InheritedNestedA top) {
        for (InheritedNestedB b : top.getSetOfB()) {
            for (InheritedNestedBaseForC c : b.getCs())
                mgr.put(c);
            mgr.put(b);
        }
        mgr.put(top);
    }

    private static InheritedNestedA generate(int off1) {
        int off = off1;
        InheritedNestedA top = new InheritedNestedA();
        top.setIdForA(off++);

        InheritedNestedB mid = new InheritedNestedB();
        mid.setIdForB(Integer.toBinaryString(off++));
        top.getSetOfB().add(mid);

        InheritedNestedC low = new InheritedNestedC();
        low.setHelloWorld("hello");
        low.setIdForC(off++);
        mid.getCs().add(low);
        low = new InheritedNestedC();
        low.setIdForC(off++);
        low.setHelloWorld("hello2");
        mid.getCs().add(low);

        mid = new InheritedNestedB();
        mid.setIdForB(Integer.toBinaryString(off++));
        top.getSetOfB().add(mid);

        low = new InheritedNestedC();
        low.setIdForC(off++);
        low.setHelloWorld("hello3");
        mid.getCs().add(low);
        low = new InheritedNestedC();
        low.setIdForC(off++);
        low.setHelloWorld("hello4");
        mid.getCs().add(low);

        return top;
    }
}
