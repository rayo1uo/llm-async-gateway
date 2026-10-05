// Command batch-controller campaigns for a fencing lock and reconciles batch jobs.
package main

import (
	"os"

	"github.com/rayo1uo/llm-async-gateway/internal/app"
)

func main() {
	os.Exit(app.Run(app.RoleControl, os.Args[1:]))
}
