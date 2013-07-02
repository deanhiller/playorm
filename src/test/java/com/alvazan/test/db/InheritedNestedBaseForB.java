/**
 * @COPYRIGHT (C) 2010 Hotel Reservation Service (HRS) Robert Ragge GmbH
 *
 * All rights reserved
 */
package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.*;

/**
 * @author Robert Stupp (last modified by $Author$)
 * @version $Revision$ $Date$
 */
@NoSqlEntity
@NoSqlInheritance(discriminatorColumnName = "discr", strategy = NoSqlInheritanceType.SINGLE_TABLE, subclassesToScan = {
        InheritedNestedB.class
})
public class InheritedNestedBaseForB {
    @NoSqlId
    private String idForB;

    public String getIdForB() {
        return idForB;
    }

    public void setIdForB(String idForB) {
        this.idForB = idForB;
    }
}
