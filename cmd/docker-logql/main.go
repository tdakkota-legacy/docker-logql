// Command main implements Docker CLI plugin to run LogQL queries.
package main

import (
	"github.com/docker/cli/cli-plugins/manager"
	"github.com/docker/cli/cli-plugins/plugin"
	"github.com/docker/cli/cli/command"
	"github.com/spf13/cobra"
	"github.com/tdakkota/docker-logql/internal/cliversion"
)

func rootCmd(dcli command.Cli) *cobra.Command {
	root := &cobra.Command{
		Use: "logql",
	}
	root.AddCommand(queryCmd(dcli))
	return root
}

func getVersion() string {
	info, _ := cliversion.GetInfo("github.com/tdakkota/docker-logql")
	switch {
	case info.Version != "":
		return info.Version
	case info.Commit != "":
		return "dev-" + info.Commit
	default:
		return "unknown"
	}
}

func main() {
	meta := manager.Metadata{
		SchemaVersion:    "0.1.0",
		Vendor:           "tdakkota",
		Version:          getVersion(),
		ShortDescription: "A simple Docker CLI plugin to run LogQL queries over docker container logs.",
		URL:              "https://github.com/tdakkota/docker-logql",
	}
	plugin.Run(rootCmd, meta)
}
