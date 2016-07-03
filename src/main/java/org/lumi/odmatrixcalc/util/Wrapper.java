package org.lumi.odmatrixcalc.util;

import java.io.Serializable;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 19/4/2016.
 */

public class Wrapper<E> implements Serializable {

    public Wrapper() {
    }

    public Wrapper(E object) {
        this.object = object;
    }

    public E getObject() {
        return object;
    }

    protected E object;
}