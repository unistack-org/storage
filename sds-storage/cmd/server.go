package cmd

import (
	"log"
	"os"

	"github.com/sdstack/storage/backend"
	"github.com/sdstack/storage/cluster"
	"github.com/sdstack/storage/kv"
	"github.com/sdstack/storage/proxy"

	// github.com/google/uuid

	"net/http"
	_ "net/http/pprof"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Control server",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: server,
}

func init() {
	rootCmd.AddCommand(serverCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// serverCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// serverCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func server(cmd *cobra.Command, args []string) {
	if viper.GetBool("debug") {
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	var clusterEngine string
	if viper.GetStringMap("cluster")["engine"] != nil {
		clusterEngine = viper.GetStringMap("cluster")["engine"].(string)
	}
	if clusterEngine == "" {
		clusterEngine = "none"
	}
	ce, err := cluster.New(clusterEngine, viper.GetStringMap("cluster")[clusterEngine])
	if err != nil {
		log.Printf("cluster start error %s", err)
		os.Exit(1)
	}

	if err = ce.Start(); err != nil {
		log.Printf("cluster start error %s", err)
		os.Exit(1)
	}
	defer ce.Stop()

	backendEngine := viper.GetStringMap("backend")["engine"].(string)
	be, err := backend.New(backendEngine, viper.GetStringMap("backend")[backendEngine])
	if err != nil {
		log.Printf("store init error %s", err)
		os.Exit(1)
	}

	engine, _ := kv.New(nil)
	engine.SetBackend(be)
	engine.SetCluster(ce)

	for _, proxyEngine := range viper.GetStringMap("proxy")["engine"].([]interface{}) {
		pe, err := proxy.New(proxyEngine.(string), viper.GetStringMap("proxy")[proxyEngine.(string)], engine)
		if err != nil {
			log.Printf("err: %s", err)
			os.Exit(1)
		}

		if err = pe.Start(); err != nil {
			log.Printf("err: %s", err)
			os.Exit(1)
		}
		defer pe.Stop()
		//conf.proxy = append(conf.proxy, proxy)
	}

	/*
		for _, apiEngine := range viper.GetStringMap("api")["engine"].([]interface{}) {
			ae, err := api.New(apiEngine.(string), viper.GetStringMap("api")[apiEngine.(string)], engine)
			if err != nil {
				log.Printf("err: %s", err)
				os.Exit(1)
			}

			if err = ae.Start(); err != nil {
				log.Printf("err: %s", err)
				os.Exit(1)
			}
			defer ae.Stop()
			//conf.proxy = append(conf.proxy, proxy)
		}
	*/
	select {}
}
