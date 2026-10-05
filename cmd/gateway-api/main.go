// Command gateway-api serves the HTTP API. It does not dispatch or reconcile.
package main

import (
	"os"

	"github.com/rayo1uo/llm-async-gateway/internal/app"
)

func main() {
	os.Exit(app.Run(app.RoleAPI, os.Args[1:]))
}
