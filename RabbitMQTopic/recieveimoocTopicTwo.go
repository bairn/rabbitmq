package main

import "rabbitmq/RabbitMQ"

func main() {
	imoocTwo := RabbitMQ.NewRabbitMQTopic("exImoocTopic", "imooc.*.two")
	imoocTwo.RecieveTopic()
}
