use std::io;

#[derive(Debug, Clone)]
pub struct UringConfig {
    pub queue_depth: u32,
    pub enable_sqpoll: bool,
    pub sq_thread_idle: u32,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            queue_depth: 512,
            enable_sqpoll: false,
            sq_thread_idle: 2000,
        }
    }
}

impl UringConfig {
    pub fn queue_depth(&self) -> u32 {
        self.queue_depth
    }

    pub fn enable_sqpoll(&self) -> bool {
        self.enable_sqpoll
    }

    pub fn sq_thread_idle(&self) -> u32 {
        self.sq_thread_idle
    }

    pub fn validate(&self) -> io::Result<()> {
        if self.queue_depth == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring queue depth must be greater than zero",
            ));
        }
        Ok(())
    }
}
