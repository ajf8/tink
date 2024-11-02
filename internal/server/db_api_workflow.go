package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/tinkerbell/tink/api/v1alpha1"
	"github.com/tinkerbell/tink/internal/deprecated/workflow"
	"github.com/tinkerbell/tink/internal/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *DBBackedServer) getActionsForTask(ctx context.Context, task *v1alpha1.Task, taskID int64) error {
	actionRows, err := s.db.QueryContext(ctx, `SELECT id, name, image, timeout, command, volumes, pid, environment, status, started_at, seconds, message
	FROM actions
	WHERE task_id=?`, taskID)
	if err != nil {
		return fmt.Errorf("failed to query DB actions:\n%+v", err)
	}
	defer actionRows.Close()
	for actionRows.Next() {
		var action v1alpha1.Action
		var actionEnvironmentJSON *string
		var actionVolumesJSON *string
		var actioncommandJSON *string
		var startedAt *string
		if err := actionRows.Scan(&action.ID, &action.Name, &action.Image, &action.Timeout, &actioncommandJSON, &actionVolumesJSON, &action.Pid, &actionEnvironmentJSON, &action.Status, &startedAt, &action.Seconds, &action.Message); err != nil {
			return fmt.Errorf("failed to scan DB action:\n%+v", err)
		}
		if startedAt != nil && len(*startedAt) > 0 {
			action.StartedAt = func() *metav1.Time {
				parsedTime, err := time.Parse(time.RFC3339, *startedAt)
				if err != nil {
					return nil
				}
				t := metav1.NewTime(parsedTime)
				return &t
			}()
		}
		if actionEnvironmentJSON != nil {
			if err := json.Unmarshal([]byte(*actionEnvironmentJSON), &action.Environment); err != nil {
				return fmt.Errorf("failed to unmarshal action environment:\n%+v", err)
			}
		}
		if actionVolumesJSON != nil {
			if err := json.Unmarshal([]byte(*actionVolumesJSON), &action.Volumes); err != nil {
				return fmt.Errorf("failed to unmarshal action volumes:\n%+v", err)
			}
		}
		if actioncommandJSON != nil {
			if err := json.Unmarshal([]byte(*actioncommandJSON), &action.Command); err != nil {
				return fmt.Errorf("failed to unmarshal action volumes:\n%+v", err)
			}
		}
		task.Actions = append(task.Actions, action)
	}
	if err := actionRows.Err(); err != nil {
		return fmt.Errorf("failed to iterate action rows: %v", err)
	}
	return nil
}

func (s *DBBackedServer) getTasksForWorkflow(ctx context.Context, wf *v1alpha1.Workflow) error {
	taskRows, err := s.db.QueryContext(ctx, `SELECT id, name, worker_addr, volumes, environment
	FROM tasks
	WHERE workflow_id=?`, wf.ID)
	if err != nil {
		return err
	}
	defer taskRows.Close()
	for taskRows.Next() {
		var task v1alpha1.Task
		var taskID int64
		var taskEnvironmentJSON *string
		var taskVolumesJSON *string
		if err := taskRows.Scan(&taskID, &task.Name, &task.WorkerAddr, &taskVolumesJSON, &taskEnvironmentJSON); err != nil {
			return fmt.Errorf("failed to scan DB task:\n%+v", err)
		}
		if taskEnvironmentJSON != nil {
			if err := json.Unmarshal([]byte(*taskEnvironmentJSON), &task.Environment); err != nil {
				return fmt.Errorf("failed to unmarshal task environment:\n%+v", err)
			}
		}
		if taskVolumesJSON != nil {
			if err := json.Unmarshal([]byte(*taskVolumesJSON), &task.Volumes); err != nil {
				return fmt.Errorf("failed to unmarshal task volumes:\n%+v", err)
			}
		}
		if err := s.getActionsForTask(ctx, &task, taskID); err != nil {
			return fmt.Errorf("failed to get actions for task:\n%+v", err)
		}
		wf.Status.Tasks = append(wf.Status.Tasks, task)
	}

	if err := taskRows.Err(); err != nil {
		return fmt.Errorf("failed to iterate task rows: %v", err)
	}

	return nil
}

func (s *DBBackedServer) getCurrentAssignedNonTerminalWorkflowsForWorker(ctx context.Context, workerID string) ([]v1alpha1.Workflow, error) {
	rows, err := s.db.QueryContext(ctx, `select id, name, namespace, kind, api_version, state, spec
		FROM workflows
		WHERE current_task_worker=? and state in ('STATE_PENDING', 'STATE_RUNNING');
	`, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var wfs []v1alpha1.Workflow
	for rows.Next() {
		var wf v1alpha1.Workflow
		var specJSON []byte
		if err := rows.Scan(&wf.ID, &wf.Name, &wf.Namespace, &wf.Kind, &wf.APIVersion, &wf.Status.State, &specJSON); err != nil {
			return nil, fmt.Errorf("failed to scan workflow: %v", err)
		}
		if err := json.Unmarshal(specJSON, &wf.Spec); err != nil {
			return nil, fmt.Errorf("failed to unmarshal workflow spec: %v", err)
		}
		wfs = append(wfs, wf)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate workflow rows: %v", err)
	}
	return wfs, nil
}

func (s *DBBackedServer) getWorkflowByName(ctx context.Context, workflowID string) (*v1alpha1.Workflow, error) {
	workflowNamespace, workflowName, _ := strings.Cut(workflowID, "/")
	rows, err := s.db.QueryContext(ctx, `select id, name, namespace, kind, api_version, state, creation_timestamp
		FROM workflows
		WHERE namespace=? AND name=?
	`, workflowNamespace, workflowName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("workflow not found")
	}
	var wf v1alpha1.Workflow
	if err != nil {
		return nil, fmt.Errorf("failed to marshal action started at:\n%+v", err)
	}
	var creationTimestamp time.Time
	if err := rows.Scan(&wf.ID, &wf.Name, &wf.Namespace, &wf.Kind, &wf.APIVersion, &wf.Status.State, &creationTimestamp); err != nil {
		return nil, err
	}
	wf.CreationTimestamp = metav1.NewTime(creationTimestamp)
	if err := s.getTasksForWorkflow(ctx, &wf); err != nil {
		return nil, fmt.Errorf("failed to get tasks for workflow: %v", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate workflows: %v", err)
	}
	return &wf, nil
}

// The following APIs are used by the worker.

func (s *DBBackedServer) WorkerWorkflowsHandlerHttp() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		wflows, err := s.getCurrentAssignedNonTerminalWorkflowsForWorker(req.Context(), vars["worker"])
		if err != nil {
			return
		}
		b, err := json.Marshal(&wflows)
		if err != nil {
			s.logger.Error(err, "could not marshal to JSON")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	}
}

func (s *DBBackedServer) WorkflowByNameHandlerHttp() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		wf, err := s.getWorkflowByName(req.Context(), vars["namespace"]+"/"+vars["name"])
		if err != nil {
			fmt.Printf("error fetching workflow: %v\n", wf)
			return
		}
		b, err := json.Marshal(&wf)
		if err != nil {
			s.logger.Error(err, "could not marshal to JSON")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	}
}

func (s *DBBackedServer) DeleteWorkflowHandlerHttp() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		result, err := s.db.ExecContext(r.Context(), `DELETE FROM workflows WHERE namespace=? AND name=?`,
			vars["namespace"], vars["name"],
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("failed to delete workflow: %v", err)))
			return
		}
		numDeletes, err := result.RowsAffected()
		if err != nil || numDeletes <= 0 {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("workflow not found"))
			return
		}
	}
}

func (s *DBBackedServer) GetWorkflowsHandlerHttp() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var wfs v1alpha1.WorkflowList
		rows, err := s.db.QueryContext(r.Context(), `select id, name, namespace, kind, api_version, state, creation_timestamp, spec
		FROM workflows`)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("failed to get workflows: %v", err)))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var wf v1alpha1.Workflow
			var creationTimestamp time.Time
			var specJSON []byte
			if err := rows.Scan(&wf.ID, &wf.Name, &wf.Namespace, &wf.Kind, &wf.APIVersion, &wf.Status.State, &creationTimestamp, &specJSON); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(fmt.Sprintf("failed to scan workflow: %v", err)))
				return
			}
			if err := json.Unmarshal(specJSON, &wf.Spec); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(fmt.Sprintf("failed to unmarshal workflow spec: %v", err)))
				return
			}
			wf.CreationTimestamp = metav1.NewTime(creationTimestamp)
			if r.URL.Query().Get("tasks") != "" {
				if err := s.getTasksForWorkflow(r.Context(), &wf); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(fmt.Sprintf("failed to get tasks for workflow: %v", err)))
					return
				}
			}
			wfs.Items = append(wfs.Items, wf)
		}
		if err := rows.Err(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		b, err := json.Marshal(&wfs)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("error marshaling workflows to JSON: %v", err)))
			return
		}
		w.Write(b)
	}
}

func (s *DBBackedServer) CreateWorkflowHandlerHttp() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var workflow v1alpha1.Workflow
		r.Body = http.MaxBytesReader(w, r.Body, 1048576)
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		err := dec.Decode(&workflow)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("bad request: %v", err)))
			return
		}
		workflow.Status.State = v1alpha1.WorkflowStatePending
		workflow.CreationTimestamp = metav1.NewTime(s.nowFunc())
		err = s.SaveWorkflow(r.Context(), &workflow)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("failed to save workflow: %v", err)))
			return
		}
		b, err := json.Marshal(&workflow)
		if err != nil {
			s.logger.Error(err, "could not marshal to JSON")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	}
}

func (s *DBBackedServer) GetWorkflowContexts(req *proto.WorkflowContextRequest, stream proto.WorkflowService_GetWorkflowContextsServer) error {
	// if spec.Netboot is true, and allowPXE: false in the hardware then don't serve a workflow context
	// if spec.ToggleHardwareNetworkBooting is true, and any associated bmc jobs dont exists or have not completed successfully then don't serve a workflow context
	if req.GetWorkerId() == "" {
		return status.Errorf(codes.InvalidArgument, errInvalidWorkflowID)
	}
	wflows, err := s.getCurrentAssignedNonTerminalWorkflowsForWorker(stream.Context(), req.WorkerId)
	if err != nil {
		return err
	}
	for _, wf := range wflows {
		// Don't serve Actions when in a v1alpha1.WorkflowStatePreparing state.
		// This is to prevent the worker from starting Actions before Workflow boot options are performed.
		// if wf.Spec.BootOptions.BootMode != "" && wf.Status.State == v1alpha1.WorkflowStatePreparing {
		// 	 continue
		// }
		if err := stream.Send(getWorkflowContext(wf)); err != nil {
			return err
		}
	}
	return nil
}

func (s *DBBackedServer) GetWorkflowActions(ctx context.Context, req *proto.WorkflowActionsRequest) (*proto.WorkflowActionList, error) {
	wfID := req.GetWorkflowId()
	if wfID == "" {
		return nil, status.Errorf(codes.InvalidArgument, errInvalidWorkflowID)
	}
	wf, err := s.getWorkflowByName(ctx, wfID)
	if err != nil {
		return nil, err
	}
	return workflow.ActionListCRDToProto(wf), nil
}

// Modifies a workflow for a given workflowContext.
func (s *DBBackedServer) modifyWorkflowState(ctx context.Context, wf *v1alpha1.Workflow, wfContext *proto.WorkflowContext) error {
	if wf == nil {
		return errors.New("no workflow provided")
	}
	if wfContext == nil {
		return errors.New("no workflow context provided")
	}
	var (
		taskIndex   = -1
		actionIndex = -1
	)

	seenActions := 0
	for ti, task := range wf.Status.Tasks {
		if wfContext.CurrentTask == task.Name {
			taskIndex = ti
			for ai, action := range task.Actions {
				if action.Name == wfContext.CurrentAction && (wfContext.CurrentActionIndex == int64(ai) || wfContext.CurrentActionIndex == int64(seenActions)) {
					actionIndex = ai
					goto cont
				}
				seenActions++
			}
		}
		seenActions += len(task.Actions)
	}
cont:

	if taskIndex < 0 {
		return errors.New("task not found")
	}
	if actionIndex < 0 {
		return errors.New("action not found")
	}

	currentAction := &wf.Status.Tasks[taskIndex].Actions[actionIndex]
	currentAction.Status = v1alpha1.WorkflowState(proto.State_name[int32(wfContext.CurrentActionState)])
	nextWorker := wf.Status.Tasks[taskIndex].WorkerAddr

	switch wfContext.CurrentActionState {
	case proto.State_STATE_RUNNING:
		// Workflow is running, so set the start time to now
		wf.Status.State = v1alpha1.WorkflowState(proto.State_name[int32(wfContext.CurrentActionState)])
		currentAction.StartedAt = func() *metav1.Time {
			t := metav1.NewTime(s.nowFunc())
			return &t
		}()
	case proto.State_STATE_FAILED, proto.State_STATE_TIMEOUT:
		// Handle terminal statuses by updating the workflow state and time
		wf.Status.State = v1alpha1.WorkflowState(proto.State_name[int32(wfContext.CurrentActionState)])
		if currentAction.StartedAt != nil {
			currentAction.Seconds = int64(s.nowFunc().Sub(currentAction.StartedAt.Time).Seconds())
		}
	case proto.State_STATE_SUCCESS:
		// Handle a success by marking the task as complete
		if currentAction.StartedAt != nil {
			currentAction.Seconds = int64(s.nowFunc().Sub(currentAction.StartedAt.Time).Seconds())
		}
		// Mark success on last action success

		if wfContext.CurrentActionIndex+1 == wfContext.TotalNumberOfActions {
			// Set the state to POST instead of Success to allow any post tasks to run.
			wf.Status.State = v1alpha1.WorkflowStatePost
			if taskIndex+1 == len(wf.Status.Tasks) {
				nextWorker = ""
			} else {
				nextWorker = wf.Status.Tasks[taskIndex+1].WorkerAddr
			}
		}

	case proto.State_STATE_PENDING:
		// This is probably a client bug?
		return errors.New("no update requested")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to init DB transaction:\n%+v", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `UPDATE workflows SET state=?, current_task_worker=? WHERE id=?`, wf.Status.State, nextWorker, wf.ID)
	if err != nil {
		return fmt.Errorf("failed to update workflow:\n%+v", err)
	}
	var startedAt string
	if currentAction.StartedAt != nil {
		startedAt = currentAction.StartedAt.Format(time.RFC3339)
	}
	_, err = tx.ExecContext(ctx, `UPDATE actions SET status=?, started_at=?, seconds=? WHERE id=?`, currentAction.Status, startedAt, currentAction.Seconds, currentAction.ID)
	if err != nil {
		return fmt.Errorf("failed to update action:\n%+v", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit workflow update:\n%+v", err)
	}
	return nil
}

func (s *DBBackedServer) ReportActionStatus(ctx context.Context, req *proto.WorkflowActionStatus) (*proto.Empty, error) {
	err := validateActionStatusRequest(req)
	if err != nil {
		return nil, err
	}
	wfID := req.GetWorkflowId()
	l := s.logger.WithValues("actionName", req.GetActionName(), "status", req.GetActionStatus(), "workflowID", req.GetWorkflowId(), "taskName", req.GetTaskName(), "worker", req.WorkerId)

	wf, err := s.getWorkflowByName(ctx, wfID)
	if err != nil {
		l.Error(err, "get workflow")
		return nil, status.Errorf(codes.InvalidArgument, errInvalidWorkflowID)
	}
	if req.GetTaskName() != wf.GetCurrentTask() {
		return nil, status.Errorf(codes.InvalidArgument, errInvalidTaskReported)
	}
	if req.GetActionName() != wf.GetCurrentAction() {
		return nil, status.Errorf(codes.InvalidArgument, errInvalidActionReported)
	}

	wfContext := getWorkflowContextForRequest(req, wf)

	err = s.modifyWorkflowState(ctx, wf, wfContext)
	if err != nil {
		l.Error(err, "modify workflow state")
		return nil, status.Errorf(codes.InvalidArgument, errInvalidWorkflowID)
	}

	return &proto.Empty{}, nil
}

func (s *DBBackedServer) SaveWorkflow(ctx context.Context, wf *v1alpha1.Workflow) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to init DB transaction:\n%+v", err)
	}
	// Defer a rollback in case anything fails.
	defer tx.Rollback()
	currentTaskWorker := ""
	if len(wf.Status.Tasks) > 0 {
		currentTaskWorker = wf.Status.Tasks[wf.GetCurrentTaskIndex()].WorkerAddr
	}
	specJSON, err := json.Marshal(wf.Spec)
	if err != nil {
		return fmt.Errorf("failed to marshal spec:\n%+v", err)
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO workflows (
		kind, api_version, name, namespace, state, global_timeout, current_task_worker, creation_timestamp, spec)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		wf.TypeMeta.Kind, wf.TypeMeta.APIVersion, wf.ObjectMeta.Name, wf.ObjectMeta.Namespace,
		wf.Status.State, wf.Status.GlobalTimeout, currentTaskWorker, wf.CreationTimestamp.Format(time.RFC3339),
		specJSON,
	)
	if err != nil {
		return fmt.Errorf("failed insert workflow:\n%+v", err)
	}

	var id int64
	if id, err = result.LastInsertId(); err != nil {
		return fmt.Errorf("failed to get inserted workflow ID:\n%+v", err)
	}

	for _, task := range wf.Status.Tasks {
		result, err := tx.ExecContext(ctx, `INSERT INTO tasks (
			name, worker_addr, workflow_id)
			VALUES (?, ?, ?)`,
			task.Name, task.WorkerAddr, id,
		)
		if err != nil {
			return fmt.Errorf("failed to insert task:\n%+v", err)
		}
		var taskID int64
		if taskID, err = result.LastInsertId(); err != nil {
			return err
		}
		if err != nil {
			return fmt.Errorf("failed to get inserted task ID:\n%+v", err)
		}
		for _, action := range task.Actions {
			commandJSON, err := json.Marshal(action.Command)
			if err != nil {
				return fmt.Errorf("failed to marshal action commands:\n%+v", err)
			}
			volumeJSON, err := json.Marshal(action.Volumes)
			if err != nil {
				return fmt.Errorf("failed to marshal action volumes:\n%+v", err)
			}
			environmentJSON, err := json.Marshal(action.Environment)
			if err != nil {
				return fmt.Errorf("failed to marshal action environment:\n%+v", err)
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO actions (
				name, image, timeout, command, volumes, pid, environment, status,
				seconds, message, task_id)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				action.Name, action.Image, action.Timeout, commandJSON, volumeJSON,
				action.Pid, environmentJSON, action.Status, action.Seconds,
				action.Message, taskID,
			)
			if err != nil {
				return fmt.Errorf("failed to insert action:\n%+v", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit workflow:\n%+v", err)
	}
	wf.ID = id

	return nil
}
