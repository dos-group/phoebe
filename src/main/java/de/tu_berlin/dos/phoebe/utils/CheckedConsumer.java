package de.tu_berlin.dos.phoebe.utils;

@FunctionalInterface
public interface CheckedConsumer<T> {

    void accept(T t) throws Exception;
}
