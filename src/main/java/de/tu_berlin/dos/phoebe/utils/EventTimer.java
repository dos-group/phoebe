package de.tu_berlin.dos.phoebe.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class EventTimer {

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class Listener {

        private static final Logger LOG = LogManager.getLogger(Listener.class);
        private static final ExecutorService service = Executors.newSingleThreadExecutor();

        private final List<Integer> indices;
        private final CheckedRunnable callback;

        public Listener(int index, CheckedRunnable callback) {

            this.indices = new ArrayList<>();
            this.indices.add(index);
            this.callback = callback;
        }

        public Listener(List<Integer> indices, CheckedRunnable callback) {

            this.indices = indices;
            this.callback = callback;
        }

        public void update() {

            service.execute(() -> {
                try {

                    this.callback.run();
                }
                catch (Throwable t) {

                    LOG.error(t);
                }
            });
        }

        public List<Integer> getIndices() {

            return this.indices;
        }
    }

    public static final Logger LOG = LogManager.getLogger(EventTimer.class);

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final AtomicInteger counter = new AtomicInteger(1);
    private final List<Listener> listeners = new ArrayList<>();

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public EventTimer() { }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public void start(int duration) throws Exception {

        this.resetCounter(0);
        for (int i = 0; i < duration; i++) {

            long currTsSec = System.currentTimeMillis() / 1000L;
            while ((System.currentTimeMillis() / 1000L) <= currTsSec) {

                new CountDownLatch(1).await(10, TimeUnit.MILLISECONDS);
            }
            this.incrementCounter();
        }
    }

    public void register(Listener listener) {

        this.listeners.add(listener);
    }

    public int getCounter() {

        return this.counter.get();
    }

    public void resetCounter(int counter) {

        this.counter.set(counter);
    }

    public void incrementCounter() {

        int newVal = this.counter.incrementAndGet();
        for (Listener listener : this.listeners) {

            listener.indices.forEach(index -> {

                if (newVal == index) listener.update();
            });

        }
    }

    public EventTimer resetListeners() {

        this.listeners.clear();
        return this;
    }
}
