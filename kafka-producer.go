package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
)

var debug bool

func PrintDebug(s string) {
	if debug {
		fmt.Println("[DEBUG]" + s)
	}
}

func usage() {
	fmt.Printf("Usage: %s -zk ZK_CONNECT [-rate NUM_MSG_PER_SEC] [-duration DURATION] [-dryrun]", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	param_help := flag.Bool("help", false, "prints usage")
	param_zk := flag.String("zk", "", "zookeeper connect string, typically zk1:2181,zk2:2181,zk3:2181/kafka")
	param_rate := flag.Int("rate", 10, "msg rate per sec")
	param_duration := flag.String("duration", "", "Time lapse to send the logs")
	//param_dryrun := flag.Bool("dryrun", false,"prints in stdout instead of kafka")
	param_debug := flag.Bool("debug", false, "Debug mode")

	debug = *param_debug
	flag.Usage = usage
	flag.Parse()
	// var validation
	if *param_help {
		usage()
	}

	if len(*param_zk) == 0 {
		fmt.Println("ERROR: parameter -zk is mandatory")
		usage()
	}

	if *param_rate <= 0 {
		fmt.Println("ERROR: --rate must be a positive Integer")
		usage()
	}

	//var duration time.Duration

	if len(*param_duration) > 0 {
		duration, err := time.ParseDuration(*param_duration)
		if err != nil {
			fmt.Println("ERROR: --duration must be a duration format (e.g. 5m, 10s, 5h")
		}
		PrintDebug(fmt.Sprint("Using duration %s", duration))
	}

	var u uuid.UUID
	for i := 1; i <= 100; i++ {
		u = uuid.New()
		fmt.Println(u.String())
	}

}
