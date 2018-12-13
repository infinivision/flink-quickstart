package org.infinivision.flink.io;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public class CompletableFutureExample {
    public static void main(String[] args) throws Exception {

        runAsync();

        // then compose
        runThenCompose();


        // then combine
        runThenCombine();


    }

    private static void runThenCompose() throws Exception {
        CompletableFuture<String> cf = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    System.out.println("Supply Async sleep 1s");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return "Supply Async";
            }
        });

        CompletableFuture<String> future = cf.thenCompose(s -> {
            return CompletableFuture.supplyAsync( () -> {
                return s + " Word";
            });
        });

        System.out.println(future.get());
    }

    private static void runThenCombine() throws Exception {
        CompletableFuture<String> cf1 = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    System.out.println("Future1 sleep 1s");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return "Future1";
            }
        });

        CompletableFuture<String> cf2 = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    System.out.println("Future2 sleep 1s");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return "Future2";
            }
        });

        CompletableFuture<String> future = cf1.thenCombine(cf2, (f1, f2) -> {
            System.out.println("Combine the two future");
            return f1+f2;
        });

        System.out.println(future.get());
    }
    private static void runAsync() throws Exception{
        CompletableFuture<String> cf = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    System.out.println("Supply Async sleep 1s");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return "Supply Async";
            }
        });

        CompletableFuture<String> cf1 = cf.thenApply((s)->{
            System.out.println("Received SupplyAsync Result");
            try {
                System.out.println("Then Supply sleep 1s");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return s + " ThenApply";
        });

        if (!cf1.isDone()) {
            System.out.println("task has not been finished");
        }

        System.out.println("result: " + cf1.get());
    }
}
