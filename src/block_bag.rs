use core::{
    ptr,
    sync::atomic::{compiler_fence, Ordering},
};

pub(crate) struct BlockBag {
    size_in_blocks: usize,
    head: *mut Block,
    pool: *mut BlockPool,
}

impl BlockBag {
    pub fn size_in_blocks(&self) -> usize {
        let mut result = 0;
        let mut curr = self.head;
        while let Some(curr_ref) = unsafe { curr.as_ref() } {
            result += 1;
            curr = curr_ref.next;
        }
        result
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        let head = unsafe { self.head.as_ref().unwrap() };
        head.next.is_null() && head.is_empty()
    }

    pub fn new(pool: *mut BlockPool) -> Self {
        let head = unsafe { pool.as_mut().unwrap() }.allocate(ptr::null_mut());
        Self {
            size_in_blocks: 1,
            head,
            pool,
        }
    }

    #[inline]
    pub fn add<T>(&mut self, obj: *mut T) {
        self.add_retired(Retired::new(obj));
    }

    #[inline]
    pub fn add_retired(&mut self, ret: Retired) {
        let head_ref = unsafe { self.head.as_mut().unwrap() };
        head_ref.push_retired(ret);
        if head_ref.is_full() {
            let new_head = unsafe { self.pool.as_mut().unwrap() }.allocate(self.head);
            self.size_in_blocks += 1;
            compiler_fence(Ordering::SeqCst);
            self.head = new_head;
        }
    }

    pub fn remove(&mut self) -> Retired {
        assert!(!self.is_empty());
        unsafe {
            let head_ref = self.head.as_mut().unwrap();
            let result;
            if head_ref.is_empty() {
                result = (*head_ref.next).pop();
                let block = self.head;
                self.head = head_ref.next;
                self.pool.as_mut().unwrap().try_recycle(block);
                self.size_in_blocks -= 1;
            } else {
                result = head_ref.pop();
            }
            result
        }
    }

    pub fn first_non_empty_block(&self) -> *mut Block {
        if self.is_empty() {
            ptr::null_mut()
        } else {
            self.head
        }
    }
}

impl Drop for BlockBag {
    fn drop(&mut self) {
        assert!(self.is_empty());
        let mut curr = self.head;
        unsafe {
            while let Some(curr_ref) = curr.as_ref() {
                self.pool.as_mut().unwrap().try_recycle(curr);
                curr = curr_ref.next;
            }
        }
    }
}

const MAX_BLOCK_POOL_SIZE: usize = 32;

pub(crate) struct BlockPool {
    pool: [*mut Block; MAX_BLOCK_POOL_SIZE],
    size: usize,
}

impl Default for BlockPool {
    fn default() -> Self {
        Self {
            pool: [ptr::null_mut(); MAX_BLOCK_POOL_SIZE],
            size: 0,
        }
    }
}

impl Drop for BlockPool {
    fn drop(&mut self) {
        for i in 0..self.size {
            let block = unsafe { &*self.pool[i] };
            assert!(block.is_empty());
            drop(unsafe { Box::from_raw(self.pool[i]) });
        }
    }
}

impl BlockPool {
    pub fn allocate(&mut self, next: *mut Block) -> *mut Block {
        unsafe {
            // If there is an available block, reuse it.
            if self.size > 0 {
                let result = self.pool[self.size - 1];
                self.size -= 1;
                *result = Block::new(next);
                result
            } else {
                Box::into_raw(Box::new(Block::new(next)))
            }
        }
    }

    pub fn try_recycle(&mut self, block: *mut Block) {
        if self.size == MAX_BLOCK_POOL_SIZE {
            drop(unsafe { Box::from_raw(block) });
        } else {
            self.pool[self.size] = block;
            self.size += 1;
        }
    }
}

// In the original implementaion, a size of Block is defined as
// (BLOCK_SIZE_DESIRED_BYTES/sizeof(T*)-3), where BLOCK_SIZE_DESIRED_BYTES is 512.
// ("-3" is presented becuase there are other three 8-byte members.)
// And it is approximately 61 because Block::data was a vector of pointers.
//
// For our implementation, Block::data also holds deleters for each record,
// so BLOCK_SIZE_DESIRED_BYTES must be divided by 16.
pub(crate) const BLOCK_SIZE_DESIRED_BYTES: usize = 512;
pub(crate) const BLOCK_SIZE: usize = BLOCK_SIZE_DESIRED_BYTES / 16 - 2;

pub(crate) struct Block {
    next: *mut Block,
    size: usize,
    data: [Retired; BLOCK_SIZE],
}

impl Block {
    pub fn new(next: *mut Block) -> Self {
        Self {
            next,
            size: 0,
            data: [Retired::default(); BLOCK_SIZE],
        }
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.size == BLOCK_SIZE
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline]
    pub fn push_retired(&mut self, ret: Retired) {
        assert!(self.size < BLOCK_SIZE);
        let prev_size = self.size;
        self.data[prev_size] = ret;
        compiler_fence(Ordering::SeqCst);
        self.size = prev_size + 1;
    }

    #[inline]
    pub fn pop(&mut self) -> Retired {
        assert!(self.size > 0);
        let ret = self.data[self.size - 1];
        self.size -= 1;
        // Used `new` instead of `new_unchecked` for debug
        ret
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        assert_eq!(self.size, 0)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Retired {
    ptr: *mut u8,
    deleter: unsafe fn(*mut u8),
}

impl Default for Retired {
    fn default() -> Self {
        Self {
            ptr: ptr::null_mut(),
            deleter: free::<u8>,
        }
    }
}

impl Retired {
    fn new<T>(ptr: *mut T) -> Self {
        Self {
            ptr: ptr as *mut u8,
            deleter: free::<T>,
        }
    }

    pub fn ptr(&self) -> *mut u8 {
        self.ptr
    }

    pub unsafe fn deallocate(self) {
        (self.deleter)(self.ptr);
    }
}

unsafe fn free<T>(ptr: *mut u8) {
    drop(Box::from_raw(ptr as *mut T));
}
