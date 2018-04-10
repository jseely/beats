package eventhub

import (
	"context"

	"github.com/Azure/azure-event-hubs-go/internal/tracing"
	"github.com/opentracing/opentracing-go"
	tag "github.com/opentracing/opentracing-go/ext"
)

func (h *Hub) startSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	tracing.ApplyComponentInfo(span)
	return span, ctx
}

func (ns *namespace) startSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	tracing.ApplyComponentInfo(span)
	return span, ctx
}

func (s *sender) startProducerSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	tracing.ApplyComponentInfo(span)
	tag.SpanKindProducer.Set(span)
	tag.MessageBusDestination.Set(span, s.getFullIdentifier())
	return span, ctx
}

func (r *receiver) startConsumerSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	tracing.ApplyComponentInfo(span)
	tag.SpanKindConsumer.Set(span)
	tag.MessageBusDestination.Set(span, r.getFullIdentifier())
	return span, ctx
}

func (r *receiver) startConsumerSpanFromWire(ctx context.Context, operationName string, reference opentracing.SpanContext, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	opts = append(opts, opentracing.FollowsFrom(reference))
	span := opentracing.StartSpan(operationName, opts...)
	ctx = opentracing.ContextWithSpan(ctx, span)
	tracing.ApplyComponentInfo(span)
	tag.SpanKindConsumer.Set(span)
	tag.MessageBusDestination.Set(span, r.getFullIdentifier())
	return span, ctx
}

func (r *receiver) startConsumerSpanFromContextFollowing(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	opts = append(opts, opentracing.FollowsFrom(opentracing.SpanFromContext(ctx).Context()))
	span := opentracing.StartSpan(operationName, opts...)
	ctx = opentracing.ContextWithSpan(ctx, span)
	tracing.ApplyComponentInfo(span)
	tag.SpanKindConsumer.Set(span)
	tag.MessageBusDestination.Set(span, r.getFullIdentifier())
	return span, ctx
}
