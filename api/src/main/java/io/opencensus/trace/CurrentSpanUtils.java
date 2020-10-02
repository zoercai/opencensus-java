/*
 * Copyright 2017, OpenCensus Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.opencensus.trace;

import io.grpc.Context;
import io.opencensus.common.Scope;
import io.opencensus.trace.unsafe.ContextUtils;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/** Util methods/functionality to interact with the {@link Span} in the {@link io.grpc.Context}. */
final class CurrentSpanUtils {
  // No instance of this class.
  private CurrentSpanUtils() {}

  /**
   * Returns The {@link Span} from the current context.
   *
   * @return The {@code Span} from the current context.
   */
  @Nullable
  static Span getCurrentSpan() {
    return ContextUtils.getValue(Context.current());
  }

  /**
   * Enters the scope of code where the given {@link Span} is in the current context, and returns an
   * object that represents that scope. The scope is exited when the returned object is closed.
   *
   * <p>Supports try-with-resource idiom.
   *
   * @param span The {@code Span} to be set to the current context.
   * @param endSpan if {@code true} the returned {@code Scope} will close the {@code Span}.
   * @param scope opentelemetry scope
   * @param otelSpan
   * @return An object that defines a scope where the given {@code Span} is set to the current
   *     context.
   */
  static Scope withSpan(Span span, boolean endSpan, io.opentelemetry.context.Scope scope,
      io.opentelemetry.trace.Span otelSpan) {
    return new ScopeInSpan(span, endSpan, scope, otelSpan);
  }

  /**
   * Wraps a {@link Runnable} so that it executes with the {@code span} as the current {@code Span}.
   *
   * @param span the {@code Span} to be set as current.
   * @param endSpan if {@code true} the returned {@code Runnable} will close the {@code Span}.
   * @param runnable the {@code Runnable} to run in the {@code Span}.
   * @return the wrapped {@code Runnable}.
   */
  static Runnable withSpan(Span span, boolean endSpan, Runnable runnable) {
    return new RunnableInSpan(span, runnable, endSpan);
  }

  /**
   * Wraps a {@link Callable} so that it executes with the {@code span} as the current {@code Span}.
   *
   * @param span the {@code Span} to be set as current.
   * @param endSpan if {@code true} the returned {@code Runnable} will close the {@code Span}.
   * @param callable the {@code Callable} to run in the {@code Span}.
   * @return the wrapped {@code Callable}.
   */
  static <C> Callable<C> withSpan(Span span, boolean endSpan, Callable<C> callable) {
    return new CallableInSpan<C>(span, callable, endSpan);
  }

  // Defines an arbitrary scope of code as a traceable operation. Supports try-with-resources idiom.
  private static final class ScopeInSpan implements Scope {
    private final Context origContext;
    private final io.opentelemetry.context.Scope otelScope;
    private final Span span;
    private final io.opentelemetry.trace.Span otelSpan;
    private final boolean endSpan;

    /**
     * Constructs a new {@link ScopeInSpan}.
     *  @param span is the {@code Span} to be added to the current {@code io.grpc.Context}.
     * @param scope otel scope
     * @param otelSpan otel span
     */
    private ScopeInSpan(Span span, boolean endSpan, io.opentelemetry.context.Scope scope,
        io.opentelemetry.trace.Span otelSpan) {
      this.span = span;
      this.otelScope = scope;
      this.endSpan = endSpan;
      this.otelSpan = otelSpan;
      origContext = ContextUtils.withValue(Context.current(), span).attach();
    }

    @Override
    public void close() {
      Context.current().detach(origContext);
      if (otelScope != null) {
        otelScope.close();
      }
      if (otelSpan != null) {
        otelSpan.end();
      }
      if (endSpan) {
        span.end();
      }
    }
  }

  private static final class RunnableInSpan implements Runnable {
    // TODO(bdrutu): Investigate if the extra private visibility increases the generated bytecode.
    private final Span span;
    private final Runnable runnable;
    private final boolean endSpan;

    private RunnableInSpan(Span span, Runnable runnable, boolean endSpan) {
      this.span = span;
      this.runnable = runnable;
      this.endSpan = endSpan;
    }

    @Override
    public void run() {
      Context origContext = ContextUtils.withValue(Context.current(), span).attach();
      try {
        runnable.run();
      } catch (Throwable t) {
        setErrorStatus(span, t);
        if (t instanceof RuntimeException) {
          throw (RuntimeException) t;
        } else if (t instanceof Error) {
          throw (Error) t;
        }
        throw new RuntimeException("unexpected", t);
      } finally {
        Context.current().detach(origContext);
        if (endSpan) {
          span.end();
        }
      }
    }
  }

  private static final class CallableInSpan<V> implements Callable<V> {
    private final Span span;
    private final Callable<V> callable;
    private final boolean endSpan;

    private CallableInSpan(Span span, Callable<V> callable, boolean endSpan) {
      this.span = span;
      this.callable = callable;
      this.endSpan = endSpan;
    }

    @Override
    public V call() throws Exception {
      Context origContext = ContextUtils.withValue(Context.current(), span).attach();
      try {
        return callable.call();
      } catch (Exception e) {
        setErrorStatus(span, e);
        throw e;
      } catch (Throwable t) {
        setErrorStatus(span, t);
        if (t instanceof Error) {
          throw (Error) t;
        }
        throw new RuntimeException("unexpected", t);
      } finally {
        Context.current().detach(origContext);
        if (endSpan) {
          span.end();
        }
      }
    }
  }

  private static void setErrorStatus(Span span, Throwable t) {
    span.setStatus(
        Status.UNKNOWN.withDescription(
            t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage()));
  }
}
