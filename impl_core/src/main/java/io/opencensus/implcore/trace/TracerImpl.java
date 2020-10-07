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

import com.google.errorprone.annotations.MustBeClosed;
import io.opencensus.common.Clock;
import io.opencensus.common.Scope;
import io.opencensus.implcore.trace.SpanBuilderImpl.ScopeInSpanWithOtelSpan;
import io.opencensus.implcore.trace.internal.RandomHandler;
import io.opencensus.internal.Utils;
import io.opencensus.trace.CurrentSpanUtils;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.export.SpanData;
import io.opentelemetry.OpenTelemetry;
import javax.annotation.Nullable;

/** Implementation of the {@link Tracer}. */
public final class TracerImpl extends Tracer {
  private final SpanBuilderImpl.Options spanBuilderOptions;

  TracerImpl(
      RandomHandler randomHandler,
      RecordEventsSpanImpl.StartEndHandler startEndHandler,
      Clock clock,
      TraceConfig traceConfig) {
    spanBuilderOptions =
        new SpanBuilderImpl.Options(randomHandler, startEndHandler, clock, traceConfig);
  }

  @MustBeClosed
  @Override
  public Scope withSpan(Span span) {
    Utils.checkNotNull(span, "span");
    if (span instanceof RecordEventsSpanImpl) {
      return new ScopeInSpanWithOtelSpan(span, false);
    }
    return CurrentSpanUtils.withSpan(Utils.checkNotNull(span, "span"), /* endSpan= */ false);
  }

  @Override
  public SpanBuilder spanBuilderWithExplicitParent(String spanName, @Nullable Span parent) {
    return SpanBuilderImpl.createWithParent(spanName, parent, spanBuilderOptions);
  }

  @Override
  public SpanBuilder spanBuilderWithRemoteParent(
      String spanName, @Nullable SpanContext remoteParentSpanContext) {
    return SpanBuilderImpl.createWithRemoteParent(
        spanName, remoteParentSpanContext, spanBuilderOptions);
  }
}
