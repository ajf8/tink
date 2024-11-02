CREATE TABLE IF NOT EXISTS "actions" (
	"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	"name" VARCHAR,
	"image" VARCHAR,
	"timeout" INTEGER,
	"command" VARCHAR,
	"volumes" VARCHAR,
	"pid" VARCHAR,
	"environment" VARCHAR,
	"status" VARCHAR DEFAULT "STATE_PENDING",
	"started_at" TIMESTAMP,
	"seconds" INTEGER,
	"message" VARCHAR,
	"task_id" INTEGER
);

CREATE INDEX actions_task_id_index ON actions(task_id);

CREATE TABLE IF NOT EXISTS "tasks" (
	"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	"name" VARCHAR,
	"worker_addr" VARCHAR,
	"volumes" VARCHAR,
	"environment" VARCHAR,
	"workflow_id" INTEGER
);

CREATE INDEX tasks_workflow_id_index ON tasks(workflow_id);

CREATE TABLE IF NOT EXISTS "workflows" (
	"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	"state" VARCHAR default "STATE_PENDING",
	"current_action" VARCHAR,
	"boot_options" VARCHAR,
	"template_rendering" VARCHAR,
	"global_timeout" INTEGER,
	"spec" VARCHAR,
	"kind" VARCHAR,
	"api_version" VARCHAR,
	"name" VARCHAR,
	"generate_name" VARCHAR,
	"namespace" VARCHAR,
	"self_link" VARCHAR,
	"uid" VARCHAR,
	"resource_version" VARCHAR,
	"generation" INTEGER,
	"creation_timestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	"deletion_timestamp" TIMESTAMP,
	"deletion_grace_period_seconds" INTEGER,
	"labels" VARCHAR,
	"annotations" VARCHAR,
	"current_task_worker" VARCHAR
);

CREATE INDEX workflows_current_task_worker_index ON workflows(current_task_worker);
CREATE INDEX workflows_state_index ON workflows(state);
CREATE UNIQUE INDEX workflows_id_index ON workflows(namespace, name);
