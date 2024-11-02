package main

import (
	"context"
	"flag"
	"time"

	"github.com/go-logr/zapr"
	"github.com/tinkerbell/tink/api/v1alpha1"
	"github.com/tinkerbell/tink/internal/server"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Config struct {
	DBDriver     string
	DBConnection string
}

var testWorkflowPendingNotStarted = &v1alpha1.Workflow{
	TypeMeta: metav1.TypeMeta{
		Kind:       "Workflow",
		APIVersion: "tinkerbell.org/v1alpha1",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name:              "debian",
		Namespace:         "default",
		CreationTimestamp: metav1.NewTime(time.Now()),
	},
	Spec: v1alpha1.WorkflowSpec{},
	Status: v1alpha1.WorkflowStatus{
		State:         v1alpha1.WorkflowStatePending,
		GlobalTimeout: 600,
		Tasks: []v1alpha1.Task{
			{
				Name:       "os-installation",
				WorkerAddr: "3c:ec:ef:4c:4f:54",
				Actions: []v1alpha1.Action{
					{
						Name:    "stream-debian-image",
						Image:   "quay.io/tinkerbell-actions/image2disk:v1.0.0",
						Timeout: 60,
						Environment: map[string]string{
							"COMPRESSED": "false",
							"DEST_DISK":  "/dev/null",
							"IMG_URL":    "https://git.kernel.org/torvalds/t/linux-6.12-rc5.tar.gz",
						},
						Status: v1alpha1.WorkflowStatePending,
					},
				},
			},
		},
	},
}

func main() {
	var c Config
	flag.StringVar(&c.DBDriver, "db-driver", "sqlite3", "The database driver to use. Only takes effect if `--backend=db`")
	flag.StringVar(&c.DBConnection, "db-connection", "tink.sqlite", "The database connection string to use. Only takes effect if `--backend=db`")

	zlog, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	logger := zapr.NewLogger(zlog).WithName("github.com/tinkerbell/tink")

	backend, err := server.NewDBBackedServer(
		logger,
		c.DBDriver,
		c.DBConnection,
	)
	if err != nil {
		panic(err)
	}
	err = backend.SaveWorkflow(context.TODO(), testWorkflowPendingNotStarted)
	if err != nil {
		panic(err)
	}
}
