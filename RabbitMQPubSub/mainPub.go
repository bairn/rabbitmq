package main

import (
	"rabbitmq/RabbitMQ"
	"strconv"
	"time"
	"fmt"
)

func main()  {
	rabbitmq := RabbitMQ.NewRabbitMQPubSub("newProduct")

	for i := 0;i< 100 ;i++ {
		rabbitmq.PublishPub("订阅模式生产地" + strconv.Itoa(i) + "条数据")
		time.Sleep(time.Second)

		fmt.Println("订阅模式生产地" + strconv.Itoa(i) + "条数据")
	}
}
