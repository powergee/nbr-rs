use std::{ptr::{self, NonNull}, sync::atomic::{compiler_fence, Ordering}};

pub(crate) struct BlockBag {
    owner: usize,
    size_in_blocks: usize,
    head: NonNull<Block>,
    tail: *mut Block,
    pool: NonNull<BlockPool>,
}

impl BlockBag {
    fn size_in_blocks(&self) -> usize {
        let mut result = 0;
        let mut curr = self.head.as_ptr();
        while let Some(curr_ref) = unsafe { curr.as_ref() } {
            result += 1;
            curr = curr_ref.next;
        }
        result
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        let head = unsafe { self.head.as_ref() };
        head.next.is_null() && head.is_empty()
    }

    pub fn new(tid: usize, mut pool: NonNull<BlockPool>) -> Self {
        let head = unsafe { pool.as_mut() }.allocate(ptr::null_mut());
        let tail = head.as_ptr();
        Self {
            owner: tid,
            size_in_blocks: 1,
            head,
            tail,
            pool,
        }
    }

    pub fn add<T>(&mut self, obj: NonNull<T>) {
        let head_ref = unsafe { self.head.as_mut() };
        head_ref.push(obj);
        if head_ref.is_full() {
            let new_head = unsafe { self.pool.as_mut() }.allocate(self.head.as_ptr());
            self.size_in_blocks += 1;
            compiler_fence(Ordering::SeqCst);
            self.head = new_head;
        }
    }

    pub fn remove<T>(&mut self) -> Retired {
        assert!(!self.is_empty());
        unsafe {
            let head_ref = self.head.as_mut();
            let result;
            if head_ref.is_empty() {
                result = (*head_ref.next).pop();
                let block = self.head;
                self.head = NonNull::new_unchecked(head_ref.next);
                self.pool.as_mut().try_recycle(block);
                self.size_in_blocks -= 1;
            } else {
                result = head_ref.pop();
            }
            result
        }
    }
}

impl Drop for BlockBag {
    fn drop(&mut self) {
        assert!(self.is_empty());
        let mut curr = self.head.as_ptr();
        unsafe {
            while let Some(curr_ref) = curr.as_ref() {
                self.pool.as_mut().try_recycle(NonNull::new_unchecked(curr));
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
    pub fn allocate(&mut self, next: *mut Block) -> NonNull<Block> {
        unsafe {
            // If there is an available block, reuse it.
            if self.size > 0 {
                let result = self.pool[self.size - 1];
                self.size -= 1;
                *result = Block::new(next);
                NonNull::new_unchecked(result)
            } else {
                NonNull::new_unchecked(Box::into_raw(Box::new(Block::new(next))))
            }
        }
    }

    pub fn try_recycle(&mut self, block: NonNull<Block>) {
        if self.size == MAX_BLOCK_POOL_SIZE {
            drop(unsafe { Box::from_raw(block.as_ptr()) });
        } else {
            self.pool[self.size] = block.as_ptr();
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
const BLOCK_SIZE_DESIRED_BYTES: usize = 512;
const BLOCK_SIZE: usize = BLOCK_SIZE_DESIRED_BYTES / 16 - 2;

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
    pub fn push<T>(&mut self, obj: NonNull<T>) {
        assert!(self.size < BLOCK_SIZE);
        let prev_size = self.size;
        self.data[prev_size] = Retired::new(obj.as_ptr());
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

    /// Linear time, however constant time to remove the last object
    #[inline]
    pub fn erase<T>(&mut self, obj: *mut T) {
        if self.size == 0 {
            return;
        }

        // A shortcut to remove the last object
        if self.data[self.size - 1].ptr == obj as _ {
            self.size -= 1;
            return;
        }

        for i in 0..self.size-1 {
            if self.data[i].ptr == obj as _ {
                self.data[i] = self.data[self.size - 1];
                compiler_fence(Ordering::SeqCst);
                self.size -= 1;
                return;
            }
        }
    }

    #[inline]
    pub fn replace<T>(&mut self, ix: usize, obj: NonNull<T>) {
        assert!(ix < self.size);
        self.data[ix] = Retired::new(obj.as_ptr());
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn clear_without_freeing(&mut self) {
        compiler_fence(Ordering::SeqCst);
        self.size = 0;
        compiler_fence(Ordering::SeqCst);
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
        Self { ptr: ptr::null_mut(), deleter: free::<u8> }
    }
}

impl Retired {
    fn new<T>(ptr: *mut T) -> Self {
        Self {
            ptr: ptr as *mut u8,
            deleter: free::<T>
        }
    }
}

unsafe fn free<T>(ptr: *mut u8) {
    drop(Box::from_raw(ptr as *mut T));
}