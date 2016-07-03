package org.lumi.odmatrixcalc.util;

import java.io.Serializable;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 4/6/2016.
 */

public class SerializableComparableWrapper<E extends Serializable & Comparable> extends SerializableWrapper<E> implements Comparable {

    public SerializableComparableWrapper() {
    }

    public SerializableComparableWrapper(E object) {
        super(object);
    }

    @Override
    public int compareTo(Object o) {
        return object.compareTo(((SerializableComparableWrapper) o).getObject());
    }

}


