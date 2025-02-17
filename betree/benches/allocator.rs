use betree_storage_stack::allocator::{
    self, Allocator, ApproximateBestFitTree, BestFitList, BestFitScan, BestFitTree, FirstFitList,
    FirstFitScan, FirstFitTree, NextFitList, NextFitScan, SegmentAllocator, WorstFitList,
    WorstFitScan, WorstFitTree, SEGMENT_SIZE_BYTES,
};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use rand::{
    distributions::{Distribution, Uniform},
    rngs::StdRng,
    SeedableRng,
};
use zipf::ZipfDistribution;

#[derive(Clone)]
enum SizeDistribution {
    Uniform(Uniform<usize>),
    Zipfian(ZipfDistribution),
}

// Define a trait for our benchmark struct to allow for trait objects
trait GenericAllocatorBenchmark {
    fn benchmark_name(&self) -> &'static str;

    fn bench_allocator_with_sync(
        &self,
        b: &mut Bencher,
        dist: SizeDistribution,
        allocations: u64,
        deallocations: u64,
        min_size: usize,
        max_size: usize,
    );
}

struct AllocatorBenchmark<A: Allocator> {
    allocator_type: std::marker::PhantomData<A>,
    benchmark_name: &'static str,
}

impl<A: Allocator + 'static> AllocatorBenchmark<A> {
    fn new(benchmark_name: &'static str) -> Self {
        AllocatorBenchmark {
            allocator_type: std::marker::PhantomData,
            benchmark_name,
        }
    }
}

impl<A: Allocator + 'static> GenericAllocatorBenchmark for AllocatorBenchmark<A> {
    fn benchmark_name(&self) -> &'static str {
        self.benchmark_name
    }

    fn bench_allocator_with_sync(
        &self,
        b: &mut Bencher,
        dist: SizeDistribution,
        allocations: u64,
        deallocations: u64,
        min_size: usize,
        max_size: usize,
    ) {
        bench_allocator_with_sync::<A>(b, dist, allocations, deallocations, min_size, max_size)
    }
}

// In Haura, allocators are not continuously active in memory. Instead, they are loaded from disk
// when needed. This benchmark simulates this behavior by creating a new allocator instance for each
// iteration. Also deallocations are buffered and applied during sync operations, not immediately to
// the allocator. Here, we simulate the sync operation by directly modifying the underlying bitmap
// data after the allocator has performed allocations, mimicking the delayed deallocation process.
fn bench_allocator_with_sync<A: Allocator>(
    b: &mut Bencher,
    dist: SizeDistribution,
    allocations: u64,
    deallocations: u64,
    min_size: usize,
    max_size: usize,
) {
    let data = [0; SEGMENT_SIZE_BYTES];
    let mut allocated = Vec::new();

    let mut rng = StdRng::seed_from_u64(42);
    let mut sample_size = || -> u32 {
        match &dist {
            SizeDistribution::Uniform(u) => return black_box(u.sample(&mut rng)) as u32,
            SizeDistribution::Zipfian(z) => {
                return (black_box(z.sample(&mut rng)) as usize).clamp(min_size, max_size) as u32
            }
        }
    };

    b.iter(|| {
        let mut allocator = A::new(data);
        for _ in 0..allocations {
            let size = sample_size();
            if let Some(offset) = black_box(allocator.allocate(size)) {
                allocated.push((offset, size));
            }
        }

        // Simulates the deferred deallocations
        let bitmap = allocator.data();
        for _ in 0..deallocations {
            if allocated.is_empty() {
                break;
            }
            let idx = rand::random::<usize>() % allocated.len();
            let (offset, size) = allocated.swap_remove(idx);

            let start = offset as usize;
            let end = (offset + size) as usize;
            let range = &mut bitmap[start..end];
            range.fill(false);
        }
        // At the end of the iteration, the allocator goes out of scope, simulating it being
        // unloaded from memory. In the next iteration, a new allocator will be created and loaded
        // with the modified bitmap data.
    });
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let min_size = 64;
    let max_size = 4096;
    let zipfian_exponent = 0.99;

    let distributions = [
        (
            "uniform",
            SizeDistribution::Uniform(Uniform::new(min_size, max_size)),
        ),
        (
            "zipfian",
            SizeDistribution::Zipfian(
                ZipfDistribution::new(max_size - min_size, zipfian_exponent).expect(""),
            ),
        ),
    ];

    // Define the allocators to benchmark
    let allocator_benchmarks: Vec<Box<dyn GenericAllocatorBenchmark>> = vec![
        Box::new(AllocatorBenchmark::<FirstFitScan>::new("first_fit_scan")),
        Box::new(AllocatorBenchmark::<FirstFitList>::new("first_fit_list")),
        Box::new(AllocatorBenchmark::<FirstFitTree>::new("first_fit_tree")),
        Box::new(AllocatorBenchmark::<NextFitScan>::new("next_fit_scan")),
        Box::new(AllocatorBenchmark::<NextFitList>::new("next_fit_list")),
        Box::new(AllocatorBenchmark::<BestFitScan>::new("best_fit_scan")),
        Box::new(AllocatorBenchmark::<BestFitList>::new("best_fit_list")),
        Box::new(AllocatorBenchmark::<ApproximateBestFitTree>::new(
            "approximate_best_fit_tree",
        )),
        Box::new(AllocatorBenchmark::<BestFitTree>::new("best_fit_tree")),
        Box::new(AllocatorBenchmark::<WorstFitScan>::new("worst_fit_scan")),
        Box::new(AllocatorBenchmark::<WorstFitList>::new("worst_fit_list")),
        Box::new(AllocatorBenchmark::<WorstFitTree>::new("worst_fit_tree")),
        Box::new(AllocatorBenchmark::<SegmentAllocator>::new("segment")),
    ];

    let alloc_dealloc_ratios = [
        (100, 50),
        (500, 250),
        (1000, 500),
        (5000, 2500),
        (10000, 5000),
    ];

    for (dist_name, dist) in distributions {
        for (allocations, deallocations) in alloc_dealloc_ratios {
            let group_name = format!("{}_sync_{}_{}", dist_name, allocations, deallocations);
            let mut group = c.benchmark_group(group_name);
            for allocator_bench in &allocator_benchmarks {
                group.bench_function(allocator_bench.benchmark_name(), |b| {
                    allocator_bench.bench_allocator_with_sync(
                        b,
                        dist.clone(),
                        allocations,
                        deallocations,
                        min_size,
                        max_size,
                    )
                });
            }
            group.finish();
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
