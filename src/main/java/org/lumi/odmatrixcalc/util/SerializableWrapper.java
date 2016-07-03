package org.lumi.odmatrixcalc.util;

import java.io.*;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 19/4/2016.
 */

public class SerializableWrapper<E extends Serializable> extends Wrapper<E> implements Serializable {

    public SerializableWrapper() {
    }

    public SerializableWrapper(E object) {
        super(object);
    }

    public static Object fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInput in = new ObjectInputStream(bis)) {
            return in.readObject();
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public byte[] toBytes() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(this);
            return bos.toByteArray();
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }


}