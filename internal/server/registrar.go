package server

import (
	"github.com/gorilla/mux"
	"google.golang.org/grpc"
)

// Registrar is an interface for registering APIs on a gRPC server.
type Registrar interface {
	Register(*grpc.Server)
	RegisterHttp(*mux.Router)
}
