package commons

import "time"

const (
	DropInDirPathDefault string        = "/tmp/irods_rule_async_exec_cmd_dropin"
	ReconnectInterval    time.Duration = 1 * time.Minute
)
