package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/resolver"
)

var (
	defaultConsulConfig = &consul.Config{
		Address:    os.Getenv("CONSUL_ADDRESS"),
		Scheme:     os.Getenv("CONSUL_SCHEME"),
		Datacenter: os.Getenv("CONSUL_DATACENTER"),
	}
	defaultQueryOption = &consul.QueryOptions{
		AllowStale: true,
	}
)

func init() {
	resolver.Register(&consulBuilder{})
}

type consulBuilder struct{}

// Build creates and starts a consul resolver that watches the name resolution
// of the target.
func (b *consulBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	if target.Endpoint == "" {
		return nil, errors.New("consul resolver: missing service name")
	}
	consulCli, err := consul.NewClient(defaultConsulConfig)
	if err != nil {
		return nil, fmt.Errorf("consul resolver: new client error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &consulResolver{
		consulCli:   consulCli,
		serviceName: target.Endpoint,
		ctx:         ctx,
		cancel:      cancel,
		cc:          cc,
		rn:          make(chan struct{}, 1),
	}
	r.wg.Add(1)
	go r.watcher()
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

// Scheme returns the naming scheme of this resolver builder, which is "consul".
func (b *consulBuilder) Scheme() string {
	return "consul"
}

type consulResolver struct {
	consulCli   *consul.Client
	serviceName string
	ctx         context.Context
	cancel      context.CancelFunc
	cc          resolver.ClientConn
	// rn channel is used by ResolveNow() to force an immediate resolution of the target.
	rn chan struct{}
	// wg is used to enforce Close() to return after the watcher() goroutine has finished.
	// Otherwise, data race will be possible. [Race Example] in dns_resolver_test we
	// replace the real lookup functions with mocked ones to facilitate testing.
	// If Close() doesn't wait for watcher() goroutine finishes, race detector sometimes
	// will warns lookup (READ the lookup function pointers) inside watcher() goroutine
	// has data race with replaceNetFunc (WRITE the lookup function pointers).
	wg sync.WaitGroup
}

// ResolveNow invoke an immediate resolution of the target that this
// consulResolver watches.
func (r *consulResolver) ResolveNow(opts resolver.ResolveNowOptions) {
	select {
	case r.rn <- struct{}{}:
	default:
	}
}

// Close closes the consulResolver.
func (r *consulResolver) Close() {
	r.cancel()
	r.wg.Wait()
}

func (r *consulResolver) watcher() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.rn:
		}
		state, err := r.lookup()
		if err != nil {
			r.cc.ReportError(err)
		} else {
			r.cc.UpdateState(*state)
		}
		t := time.NewTimer(30 * time.Second)
		select {
		case <-t.C:
		case <-r.ctx.Done():
			t.Stop()
			return
		}
	}
}

func (r *consulResolver) lookup() (*resolver.State, error) {
	srvs, _, err := r.consulCli.Catalog().Service(r.serviceName, "", defaultQueryOption)
	if err != nil {
		return nil, fmt.Errorf("consul resolver: query catalog service for %v error: %v", r.serviceName, err)
	}
	addrs := make([]resolver.Address, 0, len(srvs))
	for _, srv := range srvs {
		addrs = append(addrs, resolver.Address{Addr: fmt.Sprintf("%s:%d", srv.ServiceAddress, srv.ServicePort)})
	}
	fmt.Println(addrs)
	return &resolver.State{Addresses: addrs}, nil
}
