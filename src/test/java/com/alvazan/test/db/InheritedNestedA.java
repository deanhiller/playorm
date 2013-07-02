/**
 * @COPYRIGHT (C) 2010 Hotel Reservation Service (HRS) Robert Ragge GmbH
 *
 * All rights reserved
 */
package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Robert Stupp (last modified by $Author$)
 * @version $Revision$ $Date$
 */
@NoSqlEntity
public class InheritedNestedA {
    @NoSqlId
    private int idForA;

    @NoSqlOneToMany(columnName = "bs")
    private Set<InheritedNestedB> setOfB = new HashSet<InheritedNestedB>();

    public int getIdForA() {
        return idForA;
    }

    public void setIdForA(int idForA) {
        this.idForA = idForA;
    }

    public Set<InheritedNestedB> getSetOfB() {
        return setOfB;
    }
}
