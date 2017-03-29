package commands

import (
	cli "github.com/urfave/cli"
)

var Commands = cli.Commands{
	{
		Name:   "index",
		Usage:  "Index collection to elasticsearch",
		Action: IndexMongoToES,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "config",
				Value: "",
				Usage: "Configuration file path",
			},
			cli.IntFlag{
				Name:  "workers",
				Value: 2,
				Usage: "Number of workers to index the mongo collection",
			},
			cli.IntFlag{
				Name:  "bulk",
				Value: 1000,
				Usage: "Number of documents to index per bulk request",
			},
		},
	},
}
