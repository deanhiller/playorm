/**
 * @COPYRIGHT (C) 2010 Hotel Reservation Service (HRS) Robert Ragge GmbH
 *
 * All rights reserved
 */
package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlInheritance;
import com.alvazan.orm.api.base.anno.NoSqlInheritanceType;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlEntity;

/**
 * @author Robert Stupp (last modified by $Author$)
 * @version $Revision$ $Date$
 */
@NoSqlEntity
@NoSqlInheritance( //
        discriminatorColumnName = "discr", //
        strategy = NoSqlInheritanceType.SINGLE_TABLE,  //
        subclassesToScan = { //
                             InheritedNestedC.class })
public class InheritedNestedBaseForC {

    @NoSqlId
    private long idForC;

    public long getIdForC() {
        return idForC;
    }

    public void setIdForC(long idForC) {
        this.idForC = idForC;
    }
}
