/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api.tracing;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class MDCStream<T> implements Stream<T> {
    private final MDCContext context;
    private final Stream<T> delegate;

    public MDCStream(Stream<T> delegate) {
        this(MDCContext.current(), delegate);
    }

    public MDCStream(MDCContext context, Stream<T> delegate) {
        this.context = context;
        this.delegate = delegate;
    }

    @Override
    public Stream<T> filter(Predicate<? super T> predicate) {
        return new MDCStream<>(
                context,
                delegate.filter(new MDCPredicate<>(() -> MDCContext.of(context), predicate)));
    }

    @Override
    public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
        return new MDCStream<>(
                context, delegate.map(new MDCFunction<>(() -> MDCContext.of(context), mapper)));
    }

    @Override
    public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return new MDCStream<>(
                context, delegate.flatMap(new MDCFunction<>(() -> MDCContext.of(context), mapper)));
    }

    @Override
    public Stream<T> sorted(Comparator<? super T> comparator) {
        return new MDCStream<>(
                context,
                delegate.sorted(new MDCComparator<>(() -> MDCContext.of(context), comparator)));
    }

    @Override
    public Stream<T> peek(Consumer<? super T> action) {
        return new MDCStream<>(
                context, delegate.peek(new MDCConsumer<>(() -> MDCContext.of(context), action)));
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        delegate.forEach(new MDCConsumer<>(() -> MDCContext.of(context), action));
    }

    @Override
    public void forEachOrdered(Consumer<? super T> action) {
        delegate.forEachOrdered(new MDCConsumer<>(() -> MDCContext.of(context), action));
    }

    @Override
    public Optional<T> min(Comparator<? super T> comparator) {
        return delegate.min(new MDCComparator<>(() -> MDCContext.of(context), comparator));
    }

    @Override
    public Optional<T> max(Comparator<? super T> comparator) {
        return delegate.max(new MDCComparator<>(() -> MDCContext.of(context), comparator));
    }

    @Override
    public boolean anyMatch(Predicate<? super T> predicate) {
        return delegate.anyMatch(new MDCPredicate<>(() -> MDCContext.of(context), predicate));
    }

    @Override
    public boolean allMatch(Predicate<? super T> predicate) {
        return delegate.allMatch(new MDCPredicate<>(() -> MDCContext.of(context), predicate));
    }

    @Override
    public boolean noneMatch(Predicate<? super T> predicate) {
        return delegate.noneMatch(new MDCPredicate<>(() -> MDCContext.of(context), predicate));
    }

    @Override
    public Stream<T> onClose(Runnable closeHandler) {
        return delegate.onClose(new MDCRunnable(context, closeHandler));
    }

    @Override
    public Stream<T> sequential() {
        return new MDCStream<>(context, delegate.sequential());
    }

    @Override
    public Stream<T> parallel() {
        return new MDCStream<>(context, delegate.parallel());
    }

    @Override
    public Stream<T> unordered() {
        return new MDCStream<>(context, delegate.unordered());
    }

    @Override
    public Stream<T> distinct() {
        return new MDCStream<>(context, delegate.distinct());
    }

    @Override
    public Stream<T> sorted() {
        return new MDCStream<>(context, delegate.sorted());
    }

    @Override
    public Stream<T> limit(long maxSize) {
        return new MDCStream<>(context, delegate.limit(maxSize));
    }

    @Override
    public Stream<T> skip(long n) {
        return new MDCStream<>(context, delegate.skip(n));
    }

    @Override
    public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return delegate.flatMapToInt(new MDCFunction<>(() -> MDCContext.of(context), mapper));
    }

    @Override
    public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return delegate.flatMapToLong(new MDCFunction<>(() -> MDCContext.of(context), mapper));
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return delegate.flatMapToDouble(new MDCFunction<>(() -> MDCContext.of(context), mapper));
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super T> mapper) {
        return delegate.mapToInt(mapper);
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super T> mapper) {
        return delegate.mapToLong(mapper);
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        return delegate.mapToDouble(mapper);
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        return delegate.toArray(generator);
    }

    @Override
    public T reduce(T identity, BinaryOperator<T> accumulator) {
        return delegate.reduce(identity, accumulator);
    }

    @Override
    public Optional<T> reduce(BinaryOperator<T> accumulator) {
        return delegate.reduce(accumulator);
    }

    @Override
    public <U> U reduce(
            U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        return delegate.reduce(identity, accumulator, combiner);
    }

    @Override
    public <R> R collect(
            Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        return delegate.collect(supplier, accumulator, combiner);
    }

    @Override
    public <R, A> R collect(Collector<? super T, A, R> collector) {
        return delegate.collect(collector);
    }

    @Override
    public long count() {
        return delegate.count();
    }

    @Override
    public Optional<T> findFirst() {
        return delegate.findFirst();
    }

    @Override
    public Optional<T> findAny() {
        return delegate.findAny();
    }

    @Override
    public Iterator<T> iterator() {
        return delegate.iterator();
    }

    @Override
    public Spliterator<T> spliterator() {
        return delegate.spliterator();
    }

    @Override
    public boolean isParallel() {
        return delegate.isParallel();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
