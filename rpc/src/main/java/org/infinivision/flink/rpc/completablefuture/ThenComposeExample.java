package org.infinivision.flink.rpc.completablefuture;

import com.google.common.base.Strings;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class ThenComposeExample {
    public static void main(String[] args) throws Exception {
        String original = "Message";
        CompletableFuture<String> future1 = CompletableFuture.completedFuture(original);
        System.out.println(future1.isDone());
        CompletableFuture<String> future2 = future1.thenApply( s -> s.toUpperCase());

        CompletableFuture<String> futureDelay = CompletableFuture.completedFuture(original).thenApply(
                s -> {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return s.toLowerCase();
                }
        );



    }


}
