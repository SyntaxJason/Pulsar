package de.syntaxjason.pulsar.internal.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;

import java.util.concurrent.CompletableFuture;

/** Bridges Netty termination without ever blocking one of the endpoint's own event loops. */
public final class EndpointShutdown {
    private EndpointShutdown() { }

    public static boolean inEventLoop(EventLoopGroup group) {
        if (group != null) {
            for (EventExecutor executor : group) {
                if (executor.inEventLoop()) return true;
            }
        }
        return false;
    }

    public static CompletableFuture<Void> completion(Future<?> future) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        future.addListener(completed -> {
            if (completed.isSuccess()) result.complete(null);
            else result.completeExceptionally(completed.cause());
        });
        return result;
    }

    public static CompletableFuture<Void> shutdown(EventLoopGroup group) {
        return group == null ? CompletableFuture.completedFuture(null) : completion(group.shutdownGracefully());
    }
}
