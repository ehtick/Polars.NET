use std::io::{Write, Seek, SeekFrom, Cursor};
use std::sync::{Arc, Mutex};

use polars::prelude::file::WriteableTrait;

#[repr(C)]
pub struct FfiBuffer {
    pub data: *mut u8,
    pub len: usize,
    pub capacity: usize,
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_free_ffi_buffer(buf: FfiBuffer) {
    if !buf.data.is_null() {
        unsafe {
            let _ = Vec::from_raw_parts(buf.data, buf.len, buf.capacity);
        }
    }
}

#[derive(Clone)]
pub struct SharedMemoryWriter {
    pub buffer: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl SharedMemoryWriter {
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Cursor::new(Vec::new()))),
        }
    }
    
    // Sink 执行完毕后，用来抽取出底层 Vec<u8> 的辅助方法
    pub fn into_inner(self) -> Vec<u8> {
        // 解开 Arc -> 解开 Mutex -> 解开 Cursor
        Arc::try_unwrap(self.buffer)
            .expect("Buffer is still shared, cannot unwrap")
            .into_inner()
            .expect("Mutex poisoned")
            .into_inner()
    }
}

// 2. 为它实现 Write trait
impl Write for SharedMemoryWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.lock().unwrap().flush()
    }
}

// 3. 为它实现 Seek trait (支持 Parquet)
impl Seek for SharedMemoryWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.buffer.lock().unwrap().seek(pos)
    }
}

impl WriteableTrait for SharedMemoryWriter {
    // 模拟文件关闭。内存流无需真正关闭底层句柄，返回 Ok 即可
    fn close(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    // 模拟将所有元数据和数据刷入磁盘 (fsync)
    fn sync_all(&self) -> std::io::Result<()> {
        Ok(())
    }

    // 模拟将数据部分刷入磁盘 (fdatasync)
    fn sync_data(&self) -> std::io::Result<()> {
        Ok(())
    }
}


