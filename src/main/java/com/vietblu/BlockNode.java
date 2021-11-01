package com.vietblu;

public class BlockNode {

    private final byte[] array;
    private int used = 0;

    public BlockNode(int capacity) {
        this.array = new byte[capacity];
    }

    public byte[] getArray() {
        return array;
    }

    public int getUsed() {
        return used;
    }

    public int getCapacity() {
        return array.length;
    }

    public void put(byte[] data, int start, int len) {
        System.arraycopy(data, start, array, used, len);
        used += len;
    }

}
