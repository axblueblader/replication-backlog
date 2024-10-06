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
    private int endingOffset = 0; // increase every write equal to the length of command written
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
        endingOffset += command.length;
        spaceUsed += command.length;
        BlockNode lastNode = blockList.getLast();
        int available = lastNode.getCapacity() - lastNode.getUsed();
        if (available >= command.length) {
            lastNode.put(command, 0, command.length);
        } else {
            trimFront();
            lastNode.put(command, 0, available);
            BlockNode newNode = new BlockNode(Math.max(command.length, blockSize));
            newNode.put(command, available, command.length - available);
            blockList.addLast(newNode);
        }
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
    public int getEndingOffset() {
        return endingOffset;
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
     * Copy data from provided offset to the ending offset.
     *
     * @param startReadOffset the offset to start reading from provided by caller
     * @return byte array of containing commands
     * @throws IllegalArgumentException if offset is larger than current available or smaller than minimum available
     */
    public byte[] copyFromOffset(int startReadOffset) {
        if (startReadOffset < startingOffset || startReadOffset > endingOffset) {
            throw new IllegalArgumentException("Offset not available");
        }
        int readingOffset = startingOffset;
        Iterator<BlockNode> iter = blockList.iterator();
        int nextWriteOffset = 0;
        byte[] result = new byte[endingOffset - startReadOffset];
        while (iter.hasNext()) {
            BlockNode node = iter.next();
            readingOffset += node.getUsed();
            if (readingOffset >= startReadOffset) {
                int startPos = node.getUsed() - (readingOffset - startReadOffset);
                System.arraycopy(node.getArray(), startPos, result, nextWriteOffset, readingOffset - startReadOffset);
                nextWriteOffset +=  readingOffset - startReadOffset;
                break;
            }
        }
        while (iter.hasNext()) {
            BlockNode node = iter.next();
            System.arraycopy(node.getArray(), 0, result, nextWriteOffset, node.getUsed());
            nextWriteOffset += node.getUsed();
        }
        return result;
    }

}
