package org.infinivision.flink.rpc.completablefuture;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class CompletableFutureAcceptEither {
    public static void main(String[] args) {
        Random random = new Random();
        CompletableFuture<String> future1 = CompletableFuture.supplyAsync(
                ()-> {
                    try {
                        int sleepTime = random.nextInt(1000);
                        System.out.println("future1 sleep: " + sleepTime + 2000);
                        Thread.sleep(sleepTime+2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    return "supply async from future1";
                }
        );


        CompletableFuture<Integer> futureThenApply = future1.thenApply(s -> {
            System.out.println("future1 result: " + s);
            return 10;
        });



    }
}
