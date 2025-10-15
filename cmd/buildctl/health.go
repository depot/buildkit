package main

import (
	"fmt"

	bccommon "github.com/moby/buildkit/cmd/buildctl/common"
	"github.com/urfave/cli"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var healthCommand = cli.Command{
	Name:   "health",
	Usage:  "Checks the buildkitd gRPC health",
	Action: health,
}

func health(clicontext *cli.Context) error {
	client, err := bccommon.ResolveClient(clicontext)
	if err != nil {
		return err
	}

	healthClient := client.HealthClient()
	resp, err := healthClient.Check(bccommon.CommandContext(clicontext), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return err
	}

	fmt.Println(resp.Status.String())

	return nil
}
