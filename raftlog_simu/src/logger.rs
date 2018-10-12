use std::cell::RefCell;
use std::fmt;
use std::mem;
use std::rc::Rc;
use std::str;

use raftlog::node::NodeId;

#[derive(Clone)]
pub struct Logger {
    id: Option<NodeId>,
    indent: usize,
    newline: bool,
    buffer: Rc<RefCell<String>>,
}
impl Logger {
    pub fn new() -> Self {
        Logger {
            id: None,
            indent: 0,
            newline: true,
            buffer: Rc::new(RefCell::new(String::new())),
        }
    }
    pub fn set_id(&mut self, id: NodeId) {
        self.id = Some(id);
    }
    pub fn set_indent(&mut self, indent: usize) {
        self.indent = indent;
    }
    pub fn is_empty(&self) -> bool {
        self.buffer.borrow().is_empty()
    }
    pub fn take_buffer(&mut self) -> String {
        mem::replace(&mut *self.buffer.borrow_mut(), String::new())
    }
}
impl Default for Logger {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Write for Logger {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let mut buf = self.buffer.borrow_mut();
        if self.newline {
            self.newline = false;
            for _ in 0..self.indent {
                buf.push_str("    ");
            }
            if let Some(ref id) = self.id {
                buf.push_str(&format!("<{}>\t", id.as_str()));
            }
        }
        self.newline = s.as_bytes().last() == Some(&b'\n');
        buf.push_str(s);
        Ok(())
    }
}
