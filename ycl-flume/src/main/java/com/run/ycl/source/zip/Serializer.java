package com.run.ycl.source.zip;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public final class Serializer {
    public Serializer() {
    }

    public static byte[] javaSerialize(Object obj) {
        if (obj instanceof byte[]) {
            return (byte[])((byte[])obj);
        } else {
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(obj);
                oos.close();
                return bos.toByteArray();
            } catch (IOException var3) {
                throw new RuntimeException(var3);
            }
        }
    }
}
