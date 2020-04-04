package main

import "rabbitmq/RabbitMQ"

func main() {
	imoocTwo := RabbitMQ.NewRabbitMQRouting("exImooc", "imooc_one")
	imoocTwo.RecieveRouting()
}
