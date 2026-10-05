// Command dispatcher claims queued requests and calls the upstream.
package main

import (
	"os"

	"github.com/rayo1uo/llm-async-gateway/internal/app"
)

func main() {
	os.Exit(app.Run(app.RoleDispatch, os.Args[1:]))
}
