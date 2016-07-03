package org.lumi.odmatrixcalc.util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 4/6/2016.
 */

public class ID<E extends Serializable & Comparable> implements WritableComparable<ID>, Serializable, Comparable<ID> {

    public ID() {

    }

    public ID(E id) {
        this.id = id;
    }

    public E getId() {
        return id;
    }

    public void setId(E id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "ID{" +
                "id=" + getId().toString() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ID)) return false;

        ID<?> id1 = (ID<?>) o;

        return getId() != null ? getId().equals(id1.getId()) : id1.getId() == null;

    }

    @Override
    public int hashCode() {
        return getId() != null ? getId().hashCode() : 0;
    }

    @Override
    public int compareTo(ID o) {

        return getId().compareTo(o.getId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        SerializableWrapper<E> obj = new SerializableWrapper<>(id);
        byte[] objBytes = obj.toBytes();
        out.writeInt(objBytes.length);
        out.write(objBytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte[] objBytes = new byte[in.readInt()];
        in.readFully(objBytes);
        Object obj = null;
        try {
            obj = SerializableWrapper.fromBytes(objBytes);
            id = ((Wrapper<E>) obj).getObject();
        } catch (ClassNotFoundException e) {
        }
    }

    private E id = null;
}
