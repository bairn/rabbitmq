package main

import (
	"rabbitmq/RabbitMQ"
	"strconv"
	"fmt"
)

func main()  {
	rabbitmq := RabbitMQ.NewRabbitMQSimple("imoocSimple")

	for i:=0;i<=100000000000;i++{
		rabbitmq.PublishSimple("Hello imooc" + strconv.Itoa(i))
		//time.Sleep(time.Second)
		fmt.Println(i)
	}

}
