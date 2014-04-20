/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cobar.net.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xianmao.hexm
 */
public final class BufferPool {

    private final int chunkSize;
    private final ByteBuffer[] items;
    private final ReentrantLock lock;
    private int putIndex;
    private int takeIndex;
    private int count;
    private volatile int newCount;

    public BufferPool(int bufferSize, int chunkSize) {
        this.chunkSize = chunkSize;
        int capacity = bufferSize / chunkSize;
        capacity = (bufferSize % chunkSize == 0) ? capacity : capacity + 1;
        this.items = new ByteBuffer[capacity];
        this.lock = new ReentrantLock();
        //初始化缓冲池
        for (int i = 0; i < capacity; i++) {
            insert(create(chunkSize));
        }
    }

    public int capacity() {
        return items.length;
    }

    public int size() {
        return count;
    }

    public int getNewCount() {
        return newCount;
    }

    //从缓冲池中取出一块Buffer
    public ByteBuffer allocate() {
        ByteBuffer node = null;
        //可重入锁
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
        	//如果缓冲池剩余容量为0,则会重新分配一个Buffer
        	//在回收的时候可以将该Buffer添加到缓冲区，
        	//否则就在缓冲池中取出一块
            node = (count == 0) ? null : extract();
        } finally {
        	//该语句必须放在finally中
            lock.unlock();
        }
        
        if (node == null) {
            ++newCount;
            return create(chunkSize);
        } else {
            return node;
        }
    }

    public void recycle(ByteBuffer buffer) {
        // 拒绝回收null和容量大于chunkSize的缓存
        if (buffer == null || buffer.capacity() > chunkSize) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (count != items.length) {
                buffer.clear();
                insert(buffer);
            }
        } finally {
            lock.unlock();
        }
    }

    private void insert(ByteBuffer buffer) {
        items[putIndex] = buffer;
        putIndex = inc(putIndex);
        ++count;
    }

    private ByteBuffer extract() {
        final ByteBuffer[] items = this.items;
        ByteBuffer item = items[takeIndex];
        items[takeIndex] = null;
        takeIndex = inc(takeIndex);
        --count;
        return item;
    }

    private int inc(int i) {
        return (++i == items.length) ? 0 : i;
    }

    private ByteBuffer create(int size) {
        return ByteBuffer.allocate(size);
    }

}
