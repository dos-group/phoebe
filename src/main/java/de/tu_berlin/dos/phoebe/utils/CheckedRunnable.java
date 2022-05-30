package de.tu_berlin.dos.phoebe.utils;

@FunctionalInterface
public interface CheckedRunnable {

    void run() throws Exception;
}
