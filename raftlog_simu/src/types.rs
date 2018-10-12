//! 雑多な型定義.
use rand::distributions::range::SampleRange;
use rand::{Error, Rng, RngCore, StdRng};
use std::cell::RefCell;
use std::rc::Rc;

/// 論理尺.
pub type LogicalDuration = u64;

/// 範囲.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Range<T> {
    /// 最小値.
    pub min: T,

    /// 最大値.
    pub max: T,
}
impl<T> Range<T>
where
    T: PartialOrd + SampleRange + Copy,
{
    /// 範囲内のある値を無作為に選択する.
    pub fn choose<R: Rng>(&self, rng: &mut R) -> T {
        rng.gen_range(self.min, self.max)
    }
}

/// 確率.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Probability {
    pub prob: f64,
}
impl Probability {
    /// 指定の確率に基づき、ある事象が発生したかどうかを決定する.
    pub fn occurred<R: Rng>(self, rng: &mut R) -> bool {
        rng.gen_range(0.0, 1.0) < self.prob
    }
}

#[derive(Clone)]
pub struct SharedRng(Rc<RefCell<StdRng>>);
impl SharedRng {
    pub fn new(inner: StdRng) -> Self {
        SharedRng(Rc::new(RefCell::new(inner)))
    }
}
impl RngCore for SharedRng {
    fn next_u32(&mut self) -> u32 {
        self.0.borrow_mut().next_u32()
    }

    fn next_u64(&mut self) -> u64 {
        self.0.borrow_mut().next_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.0.borrow_mut().fill_bytes(dest)
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Error> {
        self.0.borrow_mut().try_fill_bytes(dest)
    }
}
