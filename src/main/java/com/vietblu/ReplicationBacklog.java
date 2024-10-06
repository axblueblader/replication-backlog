package com.vietblu;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * An auto trimmed double ended linked list that can be used as backlog
 */
public class ReplicationBacklog {

    private final int blockSize;
    private final int capacity;
    private final Deque<BlockNode> blockList;
    private int currentOffset = 0; // increase every write equal to the length of command written
    private int startingOffset = 0;
    private int spaceUsed = 0;

    /**
     * Instantiates a new Command log.
     *
     * @param capacity the capacity
     */
    public ReplicationBacklog(int capacity, int blockSize) {
        this.capacity = capacity;
        this.blockList = new ArrayDeque<>();
        blockList.addLast(new BlockNode(blockSize));
        this.blockSize = blockSize;
    }

    /**
     * Buffer the command, if capacity limit is reached, it will delete the oldest block
     *
     * @param command the command
     */
    public void put(byte[] command) {
        currentOffset += command.length;
        BlockNode lastNode = blockList.getLast();
        int available = lastNode.getCapacity() - lastNode.getUsed();
        if (available >= command.length) {
            lastNode.put(command, 0, command.length);
        } else {
            lastNode.put(command, 0, available);
            BlockNode newNode = new BlockNode(Math.max(command.length, blockSize));
            newNode.put(command, available, command.length - available);
            blockList.addLast(newNode);
        }
        spaceUsed += command.length;
        trimFront();
    }

    /**
     * Remove front node if memory used exceeds capacity
     */
    private void trimFront() {
        while (spaceUsed > capacity) {
            BlockNode blockNode = blockList.removeFirst();
            spaceUsed -= blockNode.getUsed();
            startingOffset += blockNode.getUsed();
        }
    }

    /**
     * Gets current offset.
     *
     * @return the current offset
     */
    public int getCurrentOffset() {
        return currentOffset;
    }

    /**
     * Gets starting offset
     *
     * @return the smallest offset available for reading
     */
    public int getStartingOffset() {
        return startingOffset;
    }

    /**
     * Copy data from provided offset to current offset.
     *
     * @param offset the offset
     * @return the array
     * @throws IllegalArgumentException if offset is larger than current available or smaller than minimum available
     */
    public byte[] copyFromOffset(int offset) {
        if (offset < startingOffset || offset > currentOffset) {
            throw new IllegalArgumentException("Offset not available");
        }
        int iterOffset = startingOffset;
        Iterator<BlockNode> iter = blockList.iterator();
        int copied = 0;
        byte[] result = new byte[currentOffset - offset];
        while (iter.hasNext()) {
            BlockNode node = iter.next();
            iterOffset += node.getUsed();
            if (iterOffset > offset) {
                int startPos = node.getUsed() - (iterOffset - offset);
                System.arraycopy(node.getArray(), startPos, result, 0, iterOffset - offset);
                copied += iterOffset - offset;
                break;
            }
        }
        while (iter.hasNext()) {
            BlockNode node = iter.next();
            System.arraycopy(node.getArray(), 0, result, copied, node.getUsed());
            copied += node.getUsed();
        }
        return result;
    }

}
