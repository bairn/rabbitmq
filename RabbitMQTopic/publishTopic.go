package main

import (
	"rabbitmq/RabbitMQ"
	"strconv"
	"time"
	"fmt"
)

func main() {
	imoocOne := RabbitMQ.NewRabbitMQTopic("exImoocTopic", "imooc.topic.one")
	imoocTwo := RabbitMQ.NewRabbitMQTopic("exImoocTopic", "imooc.topic.two")

	for i:=0;i<=1000;i++ {
		imoocOne.PublishTopic("Hello imooc one" + strconv.Itoa(i))
		imoocTwo.PublishTopic("Hello imooc Two" + strconv.Itoa(i))
		time.Sleep(time.Second)
		fmt.Println(i)
	}
}
