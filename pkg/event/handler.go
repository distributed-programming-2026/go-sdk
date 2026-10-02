package event

import "context"

type Handler interface {
	Handle(context.Context, Envelope) error
}

type HandlerFunc func(context.Context, Envelope) error

func (f HandlerFunc) Handle(ctx context.Context, envelope Envelope) error {
	return f(ctx, envelope)
}

type HandlerMiddleware func(Handler) Handler

func ChainHandler(handler Handler, middleware ...HandlerMiddleware) Handler {
	for index := len(middleware) - 1; index >= 0; index-- {
		handler = middleware[index](handler)
	}
	return handler
}
