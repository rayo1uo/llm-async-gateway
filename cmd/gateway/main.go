// Command gateway runs the API, dispatcher, and batch controller in one process.
// It remains so existing demo scripts keep working. Production replicas should
// use gateway-api, dispatcher, and batch-controller instead.
package main

import (
	"os"

	"github.com/rayo1uo/llm-async-gateway/internal/app"
)

func main() {
	os.Exit(app.Run(app.RoleAll, os.Args[1:]))
}
