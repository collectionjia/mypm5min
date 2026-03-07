use anyhow::Result;
use std::fs::File;
use std::sync::{Arc, Mutex};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use std::collections::VecDeque;
use std::io::Write;

// 全局日志缓冲区，用于 Web 显示
lazy_static::lazy_static! {
    pub static ref LOG_BUFFER: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::<String>::with_capacity(300)));
}

// 自定义 Writer，将日志写入缓冲区的同输出到 stdout
struct MultiWriter;

impl std::io::Write for MultiWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // 将日志转换为字符串并添加到缓冲区
        let log_entry = String::from_utf8_lossy(buf).to_string();
        
        if let Ok(mut buffer) = LOG_BUFFER.lock() {
            buffer.push_back(log_entry.clone());
            // 保持缓冲区大小在 300 行以内
            while buffer.len() > 300 {
                buffer.pop_front();
            }
        }
        
        // 同时也写入 stdout
        std::io::stdout().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MultiWriter {
    type Writer = MultiWriter;

    fn make_writer(&'a self) -> Self::Writer {
        MultiWriter
    }
}

pub fn init_logger() -> Result<()> {
    // 设置默认日志级别为 info，如果没有设置 RUST_LOG 环境变量
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    
    // 使用 MultiWriter
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(MultiWriter)
        .with_ansi(false) // 禁用 ANSI 颜色代码，方便 Web 显示
        .init();

    Ok(())
}
