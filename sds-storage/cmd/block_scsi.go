package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var blockSCSICmd = &cobra.Command{
	Use:   "scsi",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("block scsi called")
		fmt.Println("Available Commands:")
		for _, c := range cmd.Commands() {
			if c.IsAvailableCommand() {
				fmt.Printf("  %s\t%s\n", c.Name(), c.Short)
			}
		}
	},
}

func init() {
	blockSCSICmd.AddCommand(blockSCSIAttachCmd)
	//rootCmd.AddCommand(blockCmd)
}
