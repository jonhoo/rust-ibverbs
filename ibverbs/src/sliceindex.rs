use std::ops;

pub trait SliceIndex<T: ?Sized> {
    /// The output type returned by methods.
    type Output: ?Sized;

    /// Returns a shared reference to the output at this location, without
    /// performing any bounds checking.
    unsafe fn get_unchecked(self, slice: &T) -> &Self::Output;

    /// Returns a shared reference to the output at this location, panicking
    /// if out of bounds.
    fn index(self, slice: &T) -> &Self::Output;
}

impl<T> SliceIndex<[T]> for usize {
    type Output = T;

    #[inline]
    unsafe fn get_unchecked(self, slice: &[T]) -> &T {
        &*slice.as_ptr().add(self)
    }

    #[inline]
    fn index(self, slice: &[T]) -> &T {
        // NB: use intrinsic indexing
        &(*slice)[self]
    }
}

#[inline(never)]
#[cold]
fn slice_index_len_fail(index: usize, len: usize) -> ! {
    panic!("index {} out of range for slice of length {}", index, len);
}

#[inline(never)]
#[cold]
fn slice_index_order_fail(index: usize, end: usize) -> ! {
    panic!("slice index starts at {} but ends at {}", index, end);
}

impl<T> SliceIndex<[T]> for ops::Range<usize> {
    type Output = [T];

    #[inline]
    unsafe fn get_unchecked(self, slice: &[T]) -> &[T] {
        use std::slice::from_raw_parts;
        from_raw_parts(slice.as_ptr().add(self.start), self.end - self.start)
    }

    #[inline]
    fn index(self, slice: &[T]) -> &[T] {
        if self.start > self.end {
            slice_index_order_fail(self.start, self.end);
        } else if self.end > slice.len() {
            slice_index_len_fail(self.end, slice.len());
        }
        unsafe { self.get_unchecked(slice) }
    }
}

impl<T> SliceIndex<[T]> for ops::RangeTo<usize> {
    type Output = [T];

    #[inline]
    unsafe fn get_unchecked(self, slice: &[T]) -> &[T] {
        (0..self.end).get_unchecked(slice)
    }

    #[inline]
    fn index(self, slice: &[T]) -> &[T] {
        (0..self.end).index(slice)
    }
}

impl<T> SliceIndex<[T]> for ops::RangeFrom<usize> {
    type Output = [T];

    #[inline]
    unsafe fn get_unchecked(self, slice: &[T]) -> &[T] {
        (self.start..slice.len()).get_unchecked(slice)
    }

    #[inline]
    fn index(self, slice: &[T]) -> &[T] {
        (self.start..slice.len()).index(slice)
    }
}

impl<T> SliceIndex<[T]> for ops::RangeFull {
    type Output = [T];

    #[inline]
    unsafe fn get_unchecked(self, slice: &[T]) -> &[T] {
        slice
    }

    #[inline]
    fn index(self, slice: &[T]) -> &[T] {
        slice
    }
}

// nightly only:
//
// impl<T> SliceIndex<[T]> for ops::RangeInclusive<usize> {
//     type Output = [T];
//
//     #[inline]
//     unsafe fn get_unchecked(self, slice: &[T]) -> &[T] {
//         match self {
//             ops::RangeInclusive::Empty { .. } => &[],
//             ops::RangeInclusive::NonEmpty { start, end } => (start..end + 1).get_unchecked(slice),
//         }
//     }
//
//     #[inline]
//     fn index(self, slice: &[T]) -> &[T] {
//         match self {
//             ops::RangeInclusive::Empty { .. } => &[],
//             ops::RangeInclusive::NonEmpty { end, .. } if end == usize::max_value() => {
//                 panic!("attempted to index slice up to maximum usize");
//             }
//             ops::RangeInclusive::NonEmpty { start, end } => (start..end + 1).index(slice),
//         }
//     }
// }
//
// impl<T> SliceIndex<[T]> for ops::RangeToInclusive<usize> {
//     type Output = [T];
//
//     #[inline]
//     unsafe fn get_unchecked(self, slice: &[T]) -> &[T] {
//         (0...self.end).get_unchecked(slice)
//     }
//
//     #[inline]
//     fn index(self, slice: &[T]) -> &[T] {
//         (0...self.end).index(slice)
//     }
// }
