package com.echoxxzhang.mydb.backend.tm;

import com.echoxxzhang.mydb.backend.utils.Panic;
import com.echoxxzhang.mydb.backend.utils.Parser;
import com.echoxxzhang.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager {

    // XID文件头长度（记录管理的事务个数）
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务状态的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    // 超级事务，永远为 commited 状态(可以在没有申请事务的情况下进行一些操作)
    public static final long SUPER_XID = 0;

    static final String XID_SUFFIX = ".xid"; // XID文件后缀

    private RandomAccessFile file;
    private FileChannel fc; // 以 NIO 方式进行文件读写
    private long xidCounter;
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }


    // 检验XID是否合法
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();

        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }
        if (fileLen < LEN_XID_HEADER_LENGTH) {  // 文件长度校验
            Panic.panic(Error.BadXIDFileException);
        }
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);

        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1); // 累加
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    // 根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid) {
        // xid - 1 是除去超级事务
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    // 将XID累加，并更新到XID
    private void increXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 强制同步缓存到文件中（防止崩溃后文件丢失数据），类似BIO的flush()方法
            fc.force(false); // 布尔值表示：是否强制同步文件的元数据
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 开启事务
    @Override
    public long begin() {
        counterLock.lock(); // 上锁
        try {
            // 获取事务并且将其状态置为committed
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            increXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    // 更新事务状态
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;  // 修改状态
        ByteBuffer buf = ByteBuffer.wrap(tmp); // 将字节数组包装到缓冲区中
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 检测XID事务是否处于某status状态
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
