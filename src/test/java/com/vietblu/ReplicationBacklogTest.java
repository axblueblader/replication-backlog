package com.vietblu;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReplicationBacklogTest {

    @Test
    void whenCopyOffset_withValidDataAndOffset_returnCorrectData() {
        ReplicationBacklog backlog = new ReplicationBacklog(6, 1);
        backlog.put(new byte[]{1,});
        assertEquals(1,backlog.getCurrentOffset());
        backlog.put(new byte[]{2, 2});
        backlog.put(new byte[]{3, 3, 3});
        assertEquals(6,backlog.getCurrentOffset());
        byte[] bytes;
        bytes = backlog.copyFromOffset(0);
        assertArrayEquals(new byte[]{1, 2, 2, 3, 3, 3}, bytes);

        bytes = backlog.copyFromOffset(3);
        assertArrayEquals(new byte[]{3, 3, 3}, bytes);
    }

    @Test
    void whenCopyOffset_withDataTrimmed_returnCorrectData() {
        ReplicationBacklog backlog = new ReplicationBacklog(4, 1);
        backlog.put(new byte[]{1,});
        backlog.put(new byte[]{2, 2});
        backlog.put(new byte[]{3, 3, 3});
        byte[] bytes;
        assertEquals(3, backlog.getStartingOffset());
        bytes = backlog.copyFromOffset(3);
        assertArrayEquals(new byte[]{3, 3, 3}, bytes);

        backlog.put(new byte[]{4, 4, 4, 4});
        assertEquals(6, backlog.getStartingOffset());
        bytes = backlog.copyFromOffset(6);
        assertArrayEquals(new byte[]{4, 4, 4, 4}, bytes);
    }

    @Test
    void whenCopyOffset_withOffsetOld_shouldThrow() {
        ReplicationBacklog backlog = new ReplicationBacklog(4, 1);
        backlog.put(new byte[]{1,});
        backlog.put(new byte[]{2, 2});
        backlog.put(new byte[]{3, 3, 3});
        assertThrows(IllegalArgumentException.class, () -> {
            backlog.copyFromOffset(0);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            backlog.copyFromOffset(7);
        });
    }

    @Test
    void whenCopyOffset_withStringBytes_returnCorrectData() {
        ReplicationBacklog backlog = new ReplicationBacklog(999, 10);
        backlog.put("set a b\r\n".getBytes(StandardCharsets.UTF_8));
        backlog.put("set abc def\n".getBytes(StandardCharsets.UTF_8));

        byte[] bytes = backlog.copyFromOffset(0);
        assertEquals("set a b\r\nset abc def\n",new String(bytes));
    }
}