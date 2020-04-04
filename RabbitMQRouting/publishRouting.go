package main

import (
	"rabbitmq/RabbitMQ"
	"strconv"
	"time"
	"fmt"
)

func main() {
	imoocOne := RabbitMQ.NewRabbitMQRouting("exImooc", "imooc_one")
	imoocTwo := RabbitMQ.NewRabbitMQRouting("exImooc", "imooc_two")

	for i:=0;i<=1000;i++ {
		imoocOne.PublishRouting("Hello imooc one" + strconv.Itoa(i))
		imoocTwo.PublishRouting("Hello imooc Two" + strconv.Itoa(i))
		time.Sleep(time.Second)
		fmt.Println(i)
	}
}
