package org.infinivision.flink.io;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public class CompletableFutureExample {
    public static void main(String[] args) throws Exception {

        runAsync();

    }

    public static void runAsync() throws Exception{
        CompletableFuture cf = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    System.out.println("sleep 1s");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return "Hello World";
            }
        });

        if (!cf.isDone()) {
            System.out.println("Future was not done");
        }

        System.out.println(cf.get());

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                System.out.println("RunAsync");
            }
        });

        cf1.thenApply(new Function<Void, Object>() {
            @Override
            public Object apply(Void aVoid) {
                return "RunAsyncApply";
            }
        });
    }
}
