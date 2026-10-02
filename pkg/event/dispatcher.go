package event

import "context"

type Dispatcher interface {
	Dispatch(context.Context, Envelope) error
}

type DispatcherFunc func(context.Context, Envelope) error

func (f DispatcherFunc) Dispatch(ctx context.Context, envelope Envelope) error {
	return f(ctx, envelope)
}

type DispatcherMiddleware func(Dispatcher) Dispatcher

func ChainDispatcher(dispatcher Dispatcher, middleware ...DispatcherMiddleware) Dispatcher {
	for index := len(middleware) - 1; index >= 0; index-- {
		dispatcher = middleware[index](dispatcher)
	}
	return dispatcher
}
