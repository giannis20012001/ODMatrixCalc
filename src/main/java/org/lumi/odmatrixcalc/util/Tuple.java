package org.lumi.odmatrixcalc.util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 9/4/2016.
 */

//The elements of this list must be Writable
//For comparable Tuples the generic type E must also comparable, otherwise a ClassCastException will be thrown
public class Tuple<E extends Serializable> extends ArrayList<E> implements FullWritable<Tuple<E>>, WritableComparable<Tuple<E>> {

    public Tuple(int initialCapacity) {
        super(initialCapacity);
    }

    public Tuple() {
        super();
    }

    public Tuple(Collection<? extends E> c) {
        super(c);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        /*Write number of elements to output stream*/
        out.writeInt(super.size());

        Iterator<E> iterator = super.iterator();
        while (iterator.hasNext()) {
            SerializableWrapper<E> elem = new SerializableWrapper<>(iterator.next());
            byte[] elemBytes = elem.toBytes();
            out.writeInt(elemBytes.length);
            out.write(elemBytes);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        int i = size;

        super.ensureCapacity(size);
        super.clear();

        while (i > 0) {
            byte[] elemBytes = new byte[in.readInt()];
            in.readFully(elemBytes);

            try {
                Object obj = SerializableWrapper.fromBytes(elemBytes);
                E elem = ((Wrapper<E>) obj).getObject();
                super.add(elem);

            } catch (ClassNotFoundException e) {
                e.printStackTrace();//TODO remove this command after debugging
            }
            i--;

        }
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        //TODO
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        //TODO
    }

    @Override
    public int compareTo(Tuple<E> o) {
        for (int i = 0; i < this.size(); i++) {
            int c = ((Comparable) this.get(i)).compareTo(o.get(i));
            if (c < 0) {
                return -1;
            } else if (c > 0) {
                return 1;
            }
        }
        return 0;
    }


}