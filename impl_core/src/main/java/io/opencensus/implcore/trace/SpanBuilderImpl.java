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

package io.opencensus.implcore.trace;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.opentelemetry.trace.TracingContextUtils.currentContextWith;

import com.google.errorprone.annotations.MustBeClosed;
import io.grpc.Context;
import io.opencensus.common.Clock;
import io.opencensus.common.Scope;
import io.opencensus.implcore.internal.TimestampConverter;
import io.opencensus.implcore.trace.RecordEventsSpanImpl.StartEndHandler;
import io.opencensus.implcore.trace.internal.RandomHandler;
import io.opencensus.trace.CurrentSpanUtils;
import io.opencensus.trace.Link;
import io.opencensus.trace.Link.Type;
import io.opencensus.trace.Sampler;
import io.opencensus.trace.Span;
import io.opencensus.trace.Span.Kind;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.TraceId;
import io.opencensus.trace.TraceOptions;
import io.opencensus.trace.Tracestate;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.config.TraceParams;
import io.opencensus.trace.unsafe.ContextUtils;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;

/** Implementation of the {@link SpanBuilder}. */
final class SpanBuilderImpl extends SpanBuilder {
  private static final Tracestate TRACESTATE_DEFAULT = Tracestate.builder().build();

  private static final TraceOptions SAMPLED_TRACE_OPTIONS =
      TraceOptions.builder().setIsSampled(true).build();
  private static final TraceOptions NOT_SAMPLED_TRACE_OPTIONS =
      TraceOptions.builder().setIsSampled(false).build();

  private final Options options;
  private final String name;
  @Nullable private final Span parent;
  @Nullable private final SpanContext remoteParentSpanContext;
  @Nullable private Sampler sampler;
  private List<Span> parentLinks = Collections.<Span>emptyList();
  @Nullable private Boolean recordEvents;
  @Nullable private Kind kind;

  private Span startSpanInternal(
      @Nullable SpanContext parentContext,
      @Nullable Boolean hasRemoteParent,
      String name,
      @Nullable Sampler sampler,
      List<Span> parentLinks,
      @Nullable Boolean recordEvents,
      @Nullable Kind kind,
      @Nullable Span parentSpan) {
    TraceParams activeTraceParams = options.traceConfig.getActiveTraceParams();
    Random random = options.randomHandler.current();
    TraceId traceId;
    SpanId spanId = SpanId.generateRandomId(random);
    SpanId parentSpanId = null;
    // TODO(bdrutu): Handle tracestate correctly not just propagate.
    Tracestate tracestate = TRACESTATE_DEFAULT;
    if (parentContext == null || !parentContext.isValid()) {
      // New root span.
      traceId = TraceId.generateRandomId(random);
      // This is a root span so no remote or local parent.
      hasRemoteParent = null;
    } else {
      // New child span.
      traceId = parentContext.getTraceId();
      parentSpanId = parentContext.getSpanId();
      tracestate = parentContext.getTracestate();
    }
    TraceOptions traceOptions =
        makeSamplingDecision(
                parentContext,
                hasRemoteParent,
                name,
                sampler,
                parentLinks,
                traceId,
                spanId,
                activeTraceParams)
            ? SAMPLED_TRACE_OPTIONS
            : NOT_SAMPLED_TRACE_OPTIONS;

    if (traceOptions.isSampled() || Boolean.TRUE.equals(recordEvents)) {
      // Pass the timestamp converter from the parent to ensure that the recorded events are in
      // the right order. Implementation uses System.nanoTime() which is monotonically increasing.
      TimestampConverter timestampConverter = null;
      if (parentSpan instanceof RecordEventsSpanImpl) {
        RecordEventsSpanImpl parentRecordEventsSpan = (RecordEventsSpanImpl) parentSpan;
        timestampConverter = parentRecordEventsSpan.getTimestampConverter();
        parentRecordEventsSpan.addChild();
      }
      Span span =
          RecordEventsSpanImpl.startSpan(
              SpanContext.create(traceId, spanId, traceOptions, tracestate),
              name,
              kind,
              parentSpanId,
              hasRemoteParent,
              options.enableOtel,
              activeTraceParams,
              options.startEndHandler,
              timestampConverter,
              options.clock);
      linkSpans(span, parentLinks);
      return span;
    } else {
      return NoRecordEventsSpanImpl.create(
          SpanContext.create(traceId, spanId, traceOptions, tracestate));
    }
  }

  private static boolean makeSamplingDecision(
      @Nullable SpanContext parent,
      @Nullable Boolean hasRemoteParent,
      String name,
      @Nullable Sampler sampler,
      List<Span> parentLinks,
      TraceId traceId,
      SpanId spanId,
      TraceParams activeTraceParams) {
    // If users set a specific sampler in the SpanBuilder, use it.
    if (sampler != null) {
      return sampler.shouldSample(parent, hasRemoteParent, traceId, spanId, name, parentLinks);
    }
    // Use the default sampler if this is a root Span or this is an entry point Span (has remote
    // parent).
    if (Boolean.TRUE.equals(hasRemoteParent) || parent == null || !parent.isValid()) {
      return activeTraceParams
          .getSampler()
          .shouldSample(parent, hasRemoteParent, traceId, spanId, name, parentLinks);
    }
    // Parent is always different than null because otherwise we use the default sampler.
    return parent.getTraceOptions().isSampled() || isAnyParentLinkSampled(parentLinks);
  }

  private static boolean isAnyParentLinkSampled(List<Span> parentLinks) {
    for (Span parentLink : parentLinks) {
      if (parentLink.getContext().getTraceOptions().isSampled()) {
        return true;
      }
    }
    return false;
  }

  private static void linkSpans(Span span, List<Span> parentLinks) {
    if (!parentLinks.isEmpty()) {
      Link childLink = Link.fromSpanContext(span.getContext(), Type.CHILD_LINKED_SPAN);
      for (Span linkedSpan : parentLinks) {
        linkedSpan.addLink(childLink);
        span.addLink(Link.fromSpanContext(linkedSpan.getContext(), Type.PARENT_LINKED_SPAN));
      }
    }
  }

  static SpanBuilderImpl createWithParent(String spanName, @Nullable Span parent, Options options) {
    return new SpanBuilderImpl(spanName, null, parent, options);
  }

  static SpanBuilderImpl createWithRemoteParent(
      String spanName, @Nullable SpanContext remoteParentSpanContext, Options options) {
    return new SpanBuilderImpl(spanName, remoteParentSpanContext, null, options);
  }

  private SpanBuilderImpl(
      String name,
      @Nullable SpanContext remoteParentSpanContext,
      @Nullable Span parent,
      Options options) {
    this.name = checkNotNull(name, "name");
    this.parent = parent;
    this.remoteParentSpanContext = remoteParentSpanContext;
    this.options = options;
  }

  @Override
  public Span startSpan() {
    if (remoteParentSpanContext != null) {
      return startSpanInternal(
          remoteParentSpanContext,
          Boolean.TRUE,
          name,
          sampler,
          parentLinks,
          recordEvents,
          kind,
          null);
    } else {
      // This is not a child of a remote Span. Get the parent SpanContext from the parent Span if
      // any.
      SpanContext parentContext = null;
      Boolean hasRemoteParent = null;
      if (parent != null) {
        parentContext = parent.getContext();
        hasRemoteParent = Boolean.FALSE;
      }
      return startSpanInternal(
          parentContext, hasRemoteParent, name, sampler, parentLinks, recordEvents, kind, parent);
    }
  }

  @MustBeClosed
  @Override
  public Scope startScopedSpan() {
    Span ocSpan = startSpan();
    if (options.enableOtel && ocSpan instanceof RecordEventsSpanImpl) {
      return new ScopeInSpanWithOtelSpan(ocSpan, true);
    }
    return CurrentSpanUtils.withSpan(startSpan(), /* endSpan= */ true);
  }

  public static final class ScopeInSpanWithOtelSpan implements Scope {
    private final Context origContext;
    private final Span span;
    private final boolean endSpan;
    private io.opentelemetry.context.Scope otelScope;

    /**
     * Constructs a new {@link ScopeInSpanWithOtelSpan}.
     *
     * @param span is the {@code Span} to be added to the current {@code io.grpc.Context}.
     */
    public ScopeInSpanWithOtelSpan(Span span, boolean endSpan) {
      this.span = span;
      this.endSpan = endSpan;
      if (((RecordEventsSpanImpl) span).getOtelSpan() != null) {
        this.otelScope = currentContextWith(((RecordEventsSpanImpl) span).getOtelSpan());
      }
      origContext = ContextUtils.withValue(Context.current(), span).attach();
    }

    @Override
    public void close() {
      Context.current().detach(origContext);
      if (otelScope != null) {
        otelScope.close();
      }
      if (endSpan) {
        if (((RecordEventsSpanImpl) span).getOtelSpan() != null) {
          ((RecordEventsSpanImpl) span).getOtelSpan().end();
        }
        span.end();
      }
    }
  }

  static final class Options {
    private final RandomHandler randomHandler;
    private final RecordEventsSpanImpl.StartEndHandler startEndHandler;
    private final Clock clock;
    private final TraceConfig traceConfig;
    final boolean enableOtel;

    Options(
        RandomHandler randomHandler,
        StartEndHandler startEndHandler,
        Clock clock,
        TraceConfig traceConfig,
        boolean enableOtel) {
      this.randomHandler = checkNotNull(randomHandler, "randomHandler");
      this.startEndHandler = checkNotNull(startEndHandler, "startEndHandler");
      this.clock = checkNotNull(clock, "clock");
      this.traceConfig = checkNotNull(traceConfig, "traceConfig");
      this.enableOtel = enableOtel;
    }
  }

  @Override
  public SpanBuilderImpl setSampler(Sampler sampler) {
    this.sampler = checkNotNull(sampler, "sampler");
    return this;
  }

  @Override
  public SpanBuilderImpl setParentLinks(List<Span> parentLinks) {
    this.parentLinks = checkNotNull(parentLinks, "parentLinks");
    return this;
  }

  @Override
  public SpanBuilderImpl setRecordEvents(boolean recordEvents) {
    this.recordEvents = recordEvents;
    return this;
  }

  @Override
  public SpanBuilderImpl setSpanKind(@Nullable Kind kind) {
    this.kind = kind;
    return this;
  }
}
