package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/go-redis/redis"
	"github.com/gorilla/mux"
	"github.com/streadway/amqp"
)

type Person struct {
	Id   string `json:"id"`
	Name string `json:"name"`
	City string `json:"city"`
}

var redisClient *redis.Client
var QUEUE_NAME = "hello"
var REDIS_HOST = "redis-13910.c264.ap-south-1-1.ec2.cloud.redislabs.com:13910"
var REDIS_PASSWORD = "6XkvmDVxtZz2ChMsKRLLtp4ZYrW3XpwQ"
var REDIS_DB = 0
var AMPQ_HOST = "amqps://umgtghjv:ilO7cGhzGKowX5uKUFRFC7CLy8G1G3nQ@puffin.rmq2.cloudamqp.com/umgtghjv"

/*
	Created redis connection
	Returns redisClient
*/
func InitRedis() *redis.Client {
	rClient := redis.NewClient(&redis.Options{
		Addr:     REDIS_HOST,
		Password: REDIS_PASSWORD,
		DB:       REDIS_DB,
	})
	pong, err := rClient.Ping().Result()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(pong)
	return rClient
}

/*
	Get request body and serializes it
	Set req body id as redis key along with body as value
*/
func createPerson(res http.ResponseWriter, req *http.Request) {
	var newPerson Person

	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&newPerson)
	failOnError(err, "Decode person failed")

	serializedPerson, _ := json.Marshal(newPerson)

	if newPerson.Id == "" || newPerson.Name == "" || newPerson.City == "" {
		http.Error(res, "Incorrect Body", http.StatusBadRequest)
		return
	}

	err = redisClient.Set(newPerson.Id, string(serializedPerson), 0).Err()
	failOnError(err, "Redis set failed")

	sendMessageInQueue(newPerson.Id)
	res.WriteHeader(http.StatusCreated)
	json.NewEncoder(res).Encode("Message queued")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func sendMessageInQueue(body string) {
	//Rabbitmq connection
	conn, err := amqp.Dial(AMPQ_HOST)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	//Create channel
	channel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channel.Close()

	//Declare queue
	queue, err := channel.QueueDeclare(
		QUEUE_NAME, // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//Publish message
	err = channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
}

func main() {
	port := os.Getenv("PORT")
	redisClient = InitRedis()
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/create-person", createPerson).Methods("POST")
	log.Fatal(http.ListenAndServe(port, router))
}
