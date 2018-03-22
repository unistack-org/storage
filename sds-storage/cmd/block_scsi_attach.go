package cmd

import "github.com/spf13/cobra"

var blockSCSIAttachCmd = &cobra.Command{
	Use:   "attach",
	Short: "attach volume to scsi device",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		blockSCSIAttachAction(cmd, args)
	},
}

func blockSCSIAttachAction(cmd *cobra.Command, args []string) error {
	return nil
}
