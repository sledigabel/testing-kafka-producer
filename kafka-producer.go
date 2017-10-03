package main

import (
	"flag"
	"fmt"
	"os"
	"time"
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/google/uuid"
)

var debug bool
var dryrun bool

var msgChan chan string

func PrintDebug(s string) {
	if debug {
		fmt.Println("[DEBUG] " + s)
	}
}

func usage() {
	fmt.Printf("Usage: %s -bootstrap bookstrap servers [-rate MSG_RATE] [-duration DURATION] [-dryrun]\n\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}


//
func ProduceMessages(number int){
	ticker := time.NewTicker(time.Second)

	for range ticker.C {
		PrintDebug("BLEEP")
		for i := 1; i <= number; i++ {
			u := uuid.New()
			// TODO: make a better msg using json
			msgChan <- fmt.Sprintf("%s%s%s",u.String(),u.String(),u.String())
		}
	}
	msgChan <- "EOF"

}

func readMessages(){
	PrintDebug("Starting reading thread")
	for i := range msgChan{
		fmt.Println(i)
		if i == "EOF" {
			break
		}
	}
}

func sendMessages(w *kafka.Writer){
	// TODO
	//fmt.Println(producer,kfk_prod,topic)
	PrintDebug("Starting sending to Kafka")
	for i := range msgChan{
		if i == "EOF" {
			break
		}
		err := w.WriteMessages(context.Background(),kafka.Message{Value: []byte(i)})
		if err != nil {
			fmt.Printf("Failed to publish messages: %s\n", err)
			os.Exit(1)
		}
	}
}

func main() {
	param_help := flag.Bool("help", false, "prints usage")
	param_bootstrap := flag.String("bootstrap", "", "zookeeper connect string, typically localhost:9092")
	param_rate := flag.Int("rate", 10, "msg rate per sec")
	param_duration := flag.String("duration", "", "Time lapse to send the logs")
	param_topic := flag.String("topic","test-kafka","topic to send msgs to")
	flag.BoolVar(&dryrun,"dryrun", false,"prints in stdout instead of kafka")
	flag.BoolVar(&debug,"debug", false, "Debug mode")

	flag.Usage = usage
	flag.Parse()
	// var validation
	if *param_help {
		usage()
	}

	if len(*param_bootstrap) == 0 {
		fmt.Println("ERROR: parameter -bootstrap is mandatory")
		usage()
	}

	if *param_rate <= 0 {
		fmt.Println("ERROR: --rate must be a positive Integer")
		usage()
	}

	use_duration := false
	var duration time.Duration

	if len(*param_duration) > 0 {
		use_duration = true
		var err error
		duration, err = time.ParseDuration(*param_duration)
		if err != nil {
			fmt.Println("ERROR: --duration must be a duration format (e.g. 5m, 10s, 5h")
		}
		PrintDebug(fmt.Sprintf("Using duration %s", duration))
	}

	msgChan = make(chan string,10)

	// initiating the connection to kafka
	if !dryrun {
		PrintDebug("Connecting to Kafka")
		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{*param_bootstrap},
			Topic:   *param_topic,
			Balancer: &kafka.LeastBytes{},
		})
		defer w.Close()
		go sendMessages(w)
	} else {
		go readMessages()
	}


	if use_duration {
		timer := time.NewTimer(duration)
		PrintDebug(fmt.Sprintf("Will produce messages at %d msg/sec for %s\n",*param_rate,duration))
		go ProduceMessages(*param_rate)
		<- timer.C
	} else {
		PrintDebug(fmt.Sprintf("Will produce messages at %d msg/minutes\n",*param_rate))
		ProduceMessages(*param_rate)
	}
	//close(msgChan)

}
