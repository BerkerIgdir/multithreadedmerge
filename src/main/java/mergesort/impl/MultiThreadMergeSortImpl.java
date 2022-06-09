package mergesort.impl;

import mergesort.util.MergeSortUnit;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

//Subclasses for primitive types will be added
public class MultiThreadMergeSortImpl<T extends Comparable<T>> {
    private final T[] array;
    private final Class<T> clazz;

    protected final ExecutorService executorService = Executors.newFixedThreadPool(AVAILABLE_PROCESSORS);
    protected static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    public MultiThreadMergeSortImpl(T[] array, Class<T> clazz) {
        this.array = array;
        this.clazz = clazz;
    }

    public void printArray() {
        Arrays.stream(array).forEach(System.out::println);
    }

    public void sort() {
        List<List<Integer>> range = divideAndSortParallel();
        var sortUnits = sortingRangeHalver(range);
        mergeParallel(sortUnits);
    }

    @SuppressWarnings("unchecked")
    protected void mergeParallel(List<MergeSortUnit> sortUnits) {
        while (sortUnits.size() > 1) {
            var futures = sortUnits.stream()
                    .map(this::mergeSubmitter)
                    .map(CompletableFuture::runAsync)
                    .collect(Collectors.toList());
            futures.forEach(CompletableFuture::join);
            sortUnits = sortingUnitRangeHalver(sortUnits);
        }
        merge(array, (T[]) Array.newInstance(clazz, sortUnits.get(0).rangeEnd - sortUnits.get(0).rangeStart + 1), sortUnits.get(0).rangeStart, sortUnits.get(0).mergePoint, sortUnits.get(0).rangeEnd);
    }

    protected List<List<Integer>> divideAndSortParallel() {
        var range = sortingRangeProvider(AVAILABLE_PROCESSORS);

        var futureList = range.stream()
                .map(this::runnableSubmitter)
                .map(runnable -> CompletableFuture.runAsync(runnable, executorService).exceptionally(throwable -> {
                    executorService.shutdown();
                    throw new RuntimeException(throwable.getLocalizedMessage());
                }))
                .collect(Collectors.toList());
        futureList.forEach(CompletableFuture::join);

        executorService.shutdown();
        return range;
    }
    @SuppressWarnings("unchecked")
    private Runnable mergeSubmitter(MergeSortUnit unit) {
        return () -> merge(array, (T[]) Array.newInstance(clazz, unit.rangeEnd - unit.rangeStart + 1), unit.rangeStart, unit.mergePoint, unit.rangeEnd);
    }

    protected List<MergeSortUnit> sortingRangeHalver(List<List<Integer>> rangeToHalve) {
        var index = new AtomicInteger(0);
        return rangeToHalve.stream()
                .collect(Collectors.groupingBy(it -> index.getAndIncrement() / 2)).values()
                .stream()
                .map(this::rangeMapper)
                .collect(Collectors.toList());
    }

    protected List<MergeSortUnit> sortingUnitRangeHalver(List<MergeSortUnit> units) {
        var index = new AtomicInteger(0);
        return units.stream()
                .collect(Collectors.groupingBy(it -> index.getAndIncrement() / 2))
                .values()
                .stream()
                .map(this::unitMapper)
                .collect(Collectors.toList());
    }

    private MergeSortUnit rangeMapper(List<List<Integer>> range) {
        if (range.size() < 2) {
            return new MergeSortUnit(range.get(0).get(0), range.get(0).get(1), range.get(0).get(1));
        }

        return new MergeSortUnit(range.get(0).get(0), range.get(1).get(1), range.get(0).get(1) - range.get(0).get(0));
    }

    private MergeSortUnit unitMapper(List<MergeSortUnit> units) {
        if (units.size() < 2) {
            return units.get(0);
        }

        return new MergeSortUnit(units.get(0).rangeStart, units.get(1).rangeEnd, units.get(0).rangeEnd - units.get(0).rangeStart);
    }

    protected List<List<Integer>> sortingRangeProvider(int amountOfRangeYouNeed) {
        int basicRangeSize = array.length / amountOfRangeYouNeed;
        int numberOfAdditionalRange = array.length % amountOfRangeYouNeed;
        int firstIndex = 0;
        int lastIndex = basicRangeSize - 1;
        List<List<Integer>> result = new ArrayList<>();
        while (amountOfRangeYouNeed-- > 0) {
            if (numberOfAdditionalRange-- > 0) {
                lastIndex++;
            }
            result.add(List.of(firstIndex, lastIndex));
            firstIndex = lastIndex + 1;
            lastIndex += basicRangeSize;
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private Runnable runnableSubmitter(List<Integer> range) {
        return () -> sort(array, (T[]) Array.newInstance(clazz, range.get(1) - range.get(0) + 1), range.get(0), range.get(1));
    }

    protected void sort(T[] array, T[] aux, int lo, int hi) {
        if (lo >= hi) {
            return;
        }
        int mid = lo + (hi - lo) / 2;
        sort(array, aux, lo, mid);
        sort(array, aux, mid + 1, hi);
        merge(array, aux, lo, hi);
    }

    private void merge(T[] array, T[] aux, int lo, int hi) {
        int mid = (hi - lo) / 2;
        merge(array, aux, lo, mid, hi);
    }

    private void merge(T[] array, T[] aux, int lo, int mid, int hi) {
        System.arraycopy(array, lo, aux, 0,
                Math.min(array.length, (hi - lo + 1)));

        int i = 0, j = mid + 1;

        for (int k = lo; k <= hi; ++k) {
            if (i > mid) array[k] = aux[j++];
            else if (j > (hi - lo)) array[k] = aux[i++];
            else if (aux[i].compareTo(aux[j]) > 0) array[k] = aux[j++];
            else array[k] = aux[i++];
        }
    }

}
