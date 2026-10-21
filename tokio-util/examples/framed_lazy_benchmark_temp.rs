//! Temporary benchmark for tokio-rs/tokio#8691. Not part of the PR.
use bytes::{Buf, BufMut, BytesMut};
use futures_core::Stream;
use futures_sink::Sink;
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Instant;
use tokio_util::codec::{Decoder, Encoder, Framed, FramedRead, FramedWrite};

struct CountAllocator;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountAllocator = CountAllocator;

unsafe impl GlobalAlloc for CountAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(new_size, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[derive(Default)]
struct ByteCodec;

impl Decoder for ByteCodec {
    type Item = u8;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<u8>> {
        if src.is_empty() {
            Ok(None)
        } else {
            let byte = src[0];
            src.advance(1);
            Ok(Some(byte))
        }
    }
}

impl Encoder<u8> for ByteCodec {
    type Error = io::Error;

    fn encode(&mut self, byte: u8, dst: &mut BytesMut) -> io::Result<()> {
        dst.put_u8(black_box(byte));
        Ok(())
    }
}

fn snapshot() -> (usize, usize) {
    (
        ALLOC_COUNT.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

fn report(mode: &str, count: usize, start: Instant, previous: (usize, usize)) {
    let elapsed_ns = start.elapsed().as_nanos();
    let current = snapshot();
    println!(
        "RESULT mode={} n={} elapsed_ns={} alloc_calls={} requested_alloc_bytes={}",
        mode,
        count,
        elapsed_ns,
        current.0 - previous.0,
        current.1 - previous.1
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).expect("mode");
    let count: usize = args.get(2).expect("count").parse().unwrap();

    match mode {
        "idle_framed" => {
            let mut objects = Vec::with_capacity(count);
            let before = snapshot();
            let start = Instant::now();
            for _ in 0..count {
                objects.push(black_box(Framed::new((), ())));
            }
            report(mode, count, start, before);
            black_box(&objects);
        }
        "idle_read" => {
            let mut objects = Vec::with_capacity(count);
            let before = snapshot();
            let start = Instant::now();
            for _ in 0..count {
                objects.push(black_box(FramedRead::new((), ())));
            }
            report(mode, count, start, before);
            black_box(&objects);
        }
        "idle_write" => {
            let mut objects = Vec::with_capacity(count);
            let before = snapshot();
            let start = Instant::now();
            for _ in 0..count {
                objects.push(black_box(FramedWrite::new((), ())));
            }
            report(mode, count, start, before);
            black_box(&objects);
        }
        "steady_read" => {
            let data = vec![0x71_u8; count + 1];
            let mut reader = FramedRead::new(&data[..], ByteCodec);
            let mut cx = Context::from_waker(Waker::noop());
            match Stream::poll_next(Pin::new(&mut reader), &mut cx) {
                Poll::Ready(Some(Ok(0x71))) => (),
                other => panic!("read warm-up failed: {other:?}"),
            }
            let mut checksum: u64 = 0;
            let before = snapshot();
            let start = Instant::now();
            for _ in 0..count {
                match Stream::poll_next(Pin::new(&mut reader), &mut cx) {
                    Poll::Ready(Some(Ok(byte))) => checksum += u64::from(black_box(byte)),
                    other => panic!("read failed: {other:?}"),
                }
            }
            report(mode, count, start, before);
            black_box(checksum);
        }
        "steady_write" => {
            let mut writer = FramedWrite::new(tokio::io::sink(), ByteCodec);
            let mut cx = Context::from_waker(Waker::noop());
            Sink::<u8>::start_send(Pin::new(&mut writer), 0x71).unwrap();
            assert_eq!(writer.write_buffer().len(), 1);
            let before = snapshot();
            let start = Instant::now();
            for i in 0..count {
                match Sink::<u8>::poll_ready(Pin::new(&mut writer), &mut cx) {
                    Poll::Ready(Ok(())) => (),
                    other => panic!("writer not ready: {other:?}"),
                }
                Sink::<u8>::start_send(Pin::new(&mut writer), black_box(i as u8)).unwrap();
            }
            match Sink::<u8>::poll_flush(Pin::new(&mut writer), &mut cx) {
                Poll::Ready(Ok(())) => (),
                other => panic!("write flush failed: {other:?}"),
            }
            report(mode, count, start, before);
            black_box(writer);
        }
        other => panic!("unknown mode: {other}"),
    }
}
