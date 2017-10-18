package main

import (
	"flag"
	"fmt"
	"os"
	"time"
	"strings"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"os/signal"
	"syscall"
)

var debug bool
var dryrun bool

var msgChan chan *sarama.ProducerMessage
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


////
//func ProduceMessages(number int, timeout time.Duration, topic string, size int){
//	ticker := time.NewTicker(time.Second)
//	goodbye := time.NewTimer(timeout)
//
//	for{
//		select {
//			case <- ticker.C:
//				PrintDebug("BLEEP")
//				for i := 1; i <= number; i++ {
//					// randomize a new msg
//					u := uuid.New().String()
//					// repeat until reaching the msg size then trimming it
//					s := strings.Repeat(u, size/len(u) + 1)[:size]
//					msgChan <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(s)}
//				}
//			case <- goodbye.C:
//				PrintDebug("Timeout!")
//				PrintDebug("Finished producing")
//				done <- true
//				close(msgChan)
//				return
//		}
//	}
//	PrintDebug("Finished producing")
//	done <- true
//}

func readMessages(){
	PrintDebug("Starting reading thread")
	for i := range msgChan{
		fmt.Println(i.Value)
	}
	sent <- true
	PrintDebug("Finished reading thread")
}

func sendMessages(p sarama.AsyncProducer){
	PrintDebug("Starting sending to Kafka")
	var enqueued int
	var errors int

	for k := range msgChan {

		select {
		case err := <-p.Errors():
			errors++
			PrintDebug(fmt.Sprint("Failed to produce message:", err))
			// TODO handle this a bit more gracefully.
			p.AsyncClose()
			panic(err)
		default:
			p.Input() <- k
			enqueued++
		}
	}

	PrintDebug("Finished reading thread")
	PrintDebug(fmt.Sprintf("Processed %d msgs and %d errors",enqueued,errors))
	sent <- true
}

func main() {
	param_help := flag.Bool("help", false, "prints usage")
	param_bootstrap := flag.String("bootstrap", "", "Comma separated list, typically localhost:9092")
	param_rate := flag.Int("rate", 10, "msg rate per sec")
	param_duration := flag.String("duration", "5h", "Time lapse to send the logs")
	param_topic := flag.String("topic","test-kafka","topic to send msgs to")
	param_msgsize := flag.Int("size", 100,"Minimum message size")
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

	if *param_msgsize <= 0 {
		fmt.Println("ERROR: --size must be a positive Integer")
		usage()
	}

	var duration time.Duration
	duration, err := time.ParseDuration(*param_duration)
	if err != nil {
		fmt.Println("ERROR: --duration must be a duration format (e.g. 5m, 10s, 5h")
	}

	// creates the producer channel
	msgChan = make(chan *sarama.ProducerMessage,100000)
	done = make(chan bool)
	sent = make(chan bool)
	sigs := make(chan os.Signal)

	// initiating the connection to kafka
	if !dryrun {
		PrintDebug("Connecting to Kafka")
		PrintDebug(fmt.Sprint(strings.Split(*param_bootstrap,",")))
		config := sarama.NewConfig()
		config.Producer.Retry.Max = 5
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Partitioner = sarama.NewRandomPartitioner
		p, err := sarama.NewAsyncProducer(strings.Split(*param_bootstrap,","), config)
		if err != nil {
			panic(err)
		}

		defer p.Close()
		go sendMessages(p)
	} else {
		go readMessages()
	}

	PrintDebug(fmt.Sprintf("Will produce messages at %d msg/sec for %s\n",*param_rate,duration))
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(time.Second)
	goodbye := time.NewTimer(duration)

	MAIN:
	for{
		select {
		case <- ticker.C:
			for i := 1; i <= *param_rate ; i++ {
				// randomize a new msg
				u := uuid.New().String()
				// repeat until reaching the msg size then trimming it
				s := strings.Repeat(u, *param_msgsize/len(u) + 1)[:*param_msgsize]
				msgChan <- &sarama.ProducerMessage{Topic: *param_topic, Value: sarama.StringEncoder(s)}
			}
		case <- goodbye.C:
			PrintDebug("Finished producing")
			close(msgChan)
		case <- sigs:
			PrintDebug("Caught Interruption signal")
			close(msgChan)
			break MAIN
		}
	}
	PrintDebug("Finished producing")

	<- sent

}
