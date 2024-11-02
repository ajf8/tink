package server

import (
	"database/sql"
	"time"

	"github.com/go-logr/logr"
	"github.com/gorilla/mux"

	_ "github.com/mattn/go-sqlite3" // for database/sql
	"github.com/tinkerbell/tink/internal/proto"
	"google.golang.org/grpc"
)

func NewDBBackedServer(logger logr.Logger, dbDriver string, dbConnection string) (*DBBackedServer, error) {
	db, err := sql.Open(dbDriver, dbConnection)
	if err != nil {
		return nil, err
	}

	return &DBBackedServer{
		logger:  logger,
		db:      db,
		nowFunc: time.Now,
	}, nil
}

// DBBackedServer is a server that implements a workflow API.
type DBBackedServer struct {
	logger logr.Logger
	db     *sql.DB

	nowFunc func() time.Time
}

// Register registers the service on the gRPC server.
func (s *DBBackedServer) Register(server *grpc.Server) {
	proto.RegisterWorkflowServiceServer(server, s)
}

func (s *DBBackedServer) RegisterHttp(router *mux.Router) {
	router.HandleFunc("/workers/{worker}/workflows", s.WorkerWorkflowsHandlerHttp()).Methods("GET")
	router.HandleFunc("/workflows/{namespace}/{name}", s.WorkflowByNameHandlerHttp()).Methods("GET")
	router.HandleFunc("/workflows/{namespace}/{name}", s.DeleteWorkflowHandlerHttp()).Methods("DELETE")
	router.HandleFunc("/workflows", s.CreateWorkflowHandlerHttp()).Methods("PUT")
	router.HandleFunc("/workflows", s.GetWorkflowsHandlerHttp()).Methods("GET")
}
