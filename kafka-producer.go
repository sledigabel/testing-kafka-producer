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

var msgChan chan kafka.Message
var done chan bool
var sent chan bool


const batchSize = 2048

func PrintDebug(s string) {
	if debug {
		fmt.Println("[DEBUG] "+ time.Now().Format(time.RFC3339) + " " + s)
	}
}

func usage() {
	fmt.Printf("Usage: %s -bootstrap bookstrap servers [-rate MSG_RATE] [-duration DURATION] [-dryrun]\n\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}


//
func ProduceMessages(number int, timeout time.Duration){
	ticker := time.NewTicker(time.Second)
	goodbye := time.NewTimer(timeout)

	for{
		select {
			case <- ticker.C:
				PrintDebug("BLEEP")
				for i := 1; i <= number; i++ {
					u := uuid.New()
					// TODO: make a better msg using json
					msgChan <- kafka.Message{Value: []byte(fmt.Sprintf("%s%s%s",u.String(),u.String(),u.String()))}
				}
			case <- goodbye.C:
				PrintDebug("Timeout!")
				PrintDebug("Finished producing")
				done <- true
				close(msgChan)
				return
		}
	}
	PrintDebug("Finished producing")
	done <- true
}

func readMessages(){
	PrintDebug("Starting reading thread")
	for i := range msgChan{
		fmt.Println(string(i.Value))
	}
	sent <- true
	PrintDebug("Finished reading thread")
}

func sendMessages(w *kafka.Writer){
	// TODO
	//fmt.Println(producer,kfk_prod,topic)
	PrintDebug("Starting sending to Kafka")

	batch := make([]kafka.Message,0)

	for k := range msgChan {
		if len(batch) < batchSize {
			batch = append(batch, k)
		} else {
			PrintDebug("Sending batch full of data")
			err := w.WriteMessages(context.Background(),batch...)
			if err != nil {
				fmt.Printf("Failed to publish messages: %s\n", err)
				os.Exit(1)
			}
			batch = []kafka.Message{k}
		}
		//PrintDebug(fmt.Sprintf("Batchsize : %d",len(batch)))
	}
	PrintDebug("Ending")
	// no more messages, flush
	if len(batch) > 0{
		err := w.WriteMessages(context.Background(),batch...)
		if err != nil {
			fmt.Printf("Failed to publish messages: %s\n", err)
			os.Exit(1)
		}
	}
	PrintDebug("Finished reading thread")
	sent <- true
}

func main() {
	param_help := flag.Bool("help", false, "prints usage")
	param_bootstrap := flag.String("bootstrap", "", "zookeeper connect string, typically localhost:9092")
	param_rate := flag.Int("rate", 10, "msg rate per sec")
	param_duration := flag.String("duration", "5y", "Time lapse to send the logs")
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

	var duration time.Duration
	duration, err := time.ParseDuration(*param_duration)
	if err != nil {
		fmt.Println("ERROR: --duration must be a duration format (e.g. 5m, 10s, 5h")
	}

	msgChan = make(chan kafka.Message,100000)
	done = make(chan bool)
	sent = make(chan bool)

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

	PrintDebug(fmt.Sprintf("Will produce messages at %d msg/sec for %s\n",*param_rate,duration))
	go ProduceMessages(*param_rate,duration)

	<- done
	<- sent

}
