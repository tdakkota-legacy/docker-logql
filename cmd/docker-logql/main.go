// Command main implements Docker CLI plugin to run LogQL queries.
package main

import (
	"github.com/docker/cli/cli-plugins/manager"
	"github.com/docker/cli/cli-plugins/plugin"
	"github.com/docker/cli/cli/command"
	"github.com/spf13/cobra"
)

func rootCmd(dcli command.Cli) *cobra.Command {
	root := &cobra.Command{
		Use: "logql",
	}
	root.AddCommand(queryCmd(dcli))
	return root
}

func getVersion() string {
	return "dev"
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
