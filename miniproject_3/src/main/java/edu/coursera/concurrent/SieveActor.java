package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;

import java.util.ArrayList;
import java.util.List;

import static edu.rice.pcdp.PCDP.finish;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 */
public final class SieveActor extends Sieve {
    /**
     * {@inheritDoc}
     */
    @Override
    public int countPrimes(final int limit) {
        final SieveActorActor sieveActorActor = new SieveActorActor(2);
        finish(() -> {
            for (int i = 3; i <= limit; i += 2) {
                sieveActorActor.send(i);
            }
        });

        int numPrimes = 0;
        SieveActorActor loopActor = sieveActorActor;
        while (loopActor != null) {
            numPrimes += loopActor.getNumLocalPrimes();
            loopActor = loopActor.getNextActor();
        }
        return numPrimes;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {
        private final static int MAX_LOCAL_PRIMES = 1000;
        private final int[] localPrimes;
        private int numLocalPrimes;
        private SieveActorActor nextActor;

        SieveActorActor(final int localPrime) {
            localPrimes = new int[MAX_LOCAL_PRIMES];
            localPrimes[0] = localPrime;
            numLocalPrimes = 1;
            nextActor = null;
        }

        public int getNumLocalPrimes() {
            return numLocalPrimes;
        }

        public SieveActorActor getNextActor() {
            return nextActor;
        }

        /**
         * Process a single message sent to this actor.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {
            final int candidate = (Integer) msg;
            if (isLocallyPrime(candidate)) {
                if (numLocalPrimes < MAX_LOCAL_PRIMES) {
                    localPrimes[numLocalPrimes++] = candidate;
                } else if (nextActor == null) {
                    nextActor = new SieveActorActor(candidate);
                } else {
                    nextActor.send(msg);
                }
            }
        }

        private boolean isLocallyPrime(final int candidate) {
            boolean isPrime = true;
            for (int i = 0; i < getNumLocalPrimes(); i++) {
                if (candidate % localPrimes[i] == 0) {
                    isPrime = false;
                    break;
                }
            }
            return isPrime;
        }
    }
}
