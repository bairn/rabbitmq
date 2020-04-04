package RabbitMQ

import (
	"github.com/streadway/amqp"
	"log"
	"fmt"
)

const MQURL  = "amqp://imoocuser:imoocuser@127.0.0.1:5672/imooc"

type RabbitMQ struct {
	conn *amqp.Connection
	channel *amqp.Channel
	QueueName string
	Exchange string
	Key string
	Mqurl string
}

func NewRabbitMQ (queueName string, exchange string, key string) *RabbitMQ {
	rabbitmq := &RabbitMQ{
		QueueName:queueName,
		Exchange:exchange,
		Key:key,
		Mqurl:MQURL,
	}

	return rabbitmq
}

func (r *RabbitMQ) Destory() {
	r.channel.Close()
	r.conn.Close()
}

func (r *RabbitMQ) failOnErr(err error, message string) {
	if err != nil {
		log.Fatalf("%s:%s", message, err)
		panic(fmt.Sprintf("%s:%s", message, err))
	}
}

func NewRabbitMQSimple(queueName string) *RabbitMQ {
	rabbitmq := NewRabbitMQ(queueName, "", "")

	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "创建连接错误")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "获取channel失败")
	return rabbitmq
}

func (r *RabbitMQ) PublishSimple(message string) {
	//申请队列，如果不存在会自动创建
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		//是否持久化
		false,
		//是否自动删除，当我们最后一个消费者从连接里断开后
		false,
		//是否具有排他性
		false,
		//是否阻塞
		false,
		nil,
	)

	if err != nil {
		fmt.Println(err)
	}

	r.channel.Publish(
		r.Exchange,
		r.QueueName,
		//如果为true，根据exchange类型和route key 规则，如果无法找到符合条件的队列那么会把发送的消息返回给发送者
		false,
		//如果为true，当exchange发送消息导队列后发现队列上没有绑定消费者，则会把消息发还给发送着
		false,
		amqp.Publishing{
			ContentType:"text/plain",
			Body:[]byte(message),
		},
	)
}

func (r *RabbitMQ) ConsumeSimple() {
	//申请队列，如果不存在会自动创建
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		//是否持久化
		false,
		//是否自动删除，当我们最后一个消费者从连接里断开后
		false,
		//是否具有排他性
		false,
		//是否阻塞
		false,
		//额外属性
		nil,
	)

	if err != nil {
		fmt.Println(err)
	}

	msgs, err := r.channel.Consume(
		r.QueueName,
		//用来区分多个消费者
		"",
		//是否自动应答
		true,
		//是否具有排他性
		false,
		//如果设置为true，表示不能将同一个connection中发送的消息传递给这个connection中的消费者
		false,
		//队列消费是否阻塞
		false,
		nil,
	)

	if err != nil {
		fmt.Println(err)
	}

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("Receibed a message")
			fmt.Println(string(d.Body))
		}
	}()

	log.Printf("[*] Waiting for message , To exit press CTRL+C")

	<- forever
}

func NewRabbitMQPubSub(exchangeName string) *RabbitMQ {
	rabbitmq := NewRabbitMQ("", exchangeName, "")
	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "failed to connect rabbitmq")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "failed to open a channel")
	return rabbitmq
}

//订阅模式生产
func (r *RabbitMQ) PublishPub(message string) {

	//尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		//广播类型
		"fanout",
		true,
		false,
		//YES表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		false,
		nil,
	)

	r.failOnErr(err, "failed to declare an exchange")

	r.channel.Publish(
		r.Exchange,
		"",
		//如果为true，根据exchange类型和route key 规则，如果无法找到符合条件的队列那么会把发送的消息返回给发送者
		false,
		//如果为true，当exchange发送消息导队列后发现队列上没有绑定消费者，则会把消息发还给发送着
		false,
		amqp.Publishing{
			ContentType:"text/plain",
			Body:[]byte(message),
		},
	)
}

func (r *RabbitMQ) RecieveSub() {
	//尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		//广播类型
		"fanout",
		true,
		false,
		//YES表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		false,
		nil,
	)

	//试探性的创建队列，这里的队列名称不要写
	q, err := r.channel.QueueDeclare(
		"", //随机生成队列名称
		false,
		false,
		true, //是否排他
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare a queue")

	err = r.channel.QueueBind(
		q.Name,
		//pub/sub模式下，这里的key要为空
		"",
		r.Exchange,
		false,
		nil,
	)
	r.failOnErr(err, "Failed QueueBind")

	messges, err := r.channel.Consume(
		q.Name,
		//用来区分多个消费者
		"",
		//是否自动应答
		true,
		//是否具有排他性
		false,
		//如果设置为true，表示不能将同一个connection中发送的消息传递给这个connection中的消费者
		false,
		//队列消费是否阻塞
		false,
		nil,
	)

	forever := make(chan bool)

	go func() {
		for d := range messges {
			log.Printf("Received a message:%s", d.Body)
		}
	}()

	fmt.Println("推出请按 CTRL+C\n")

	<- forever

}


//路由模式
//创建RabbitMQ实例
func NewRabbitMQRouting(exchangeName string, routingKey string) *RabbitMQ {
	rabbitmq := NewRabbitMQ("", exchangeName, routingKey)
	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "failed to connect rabbitmq")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "failed to open a channel")
	return rabbitmq
}

func (r *RabbitMQ) PublishRouting(message string) {
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)

	r.failOnErr(err, "failed to declare an exchange")

	r.channel.Publish(
		r.Exchange,
		r.Key,
		//如果为true，根据exchange类型和route key 规则，如果无法找到符合条件的队列那么会把发送的消息返回给发送者
		false,
		//如果为true，当exchange发送消息导队列后发现队列上没有绑定消费者，则会把消息发还给发送着
		false,
		amqp.Publishing{
			ContentType:"text/plain",
			Body:[]byte(message),
		},
	)
}


func (r *RabbitMQ) RecieveRouting() {
	//尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		//广播类型
		"direct",
		true,
		false,
		//YES表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		false,
		nil,
	)

	//试探性的创建队列，这里的队列名称不要写
	q, err := r.channel.QueueDeclare(
		"", //随机生成队列名称
		false,
		false,
		true, //是否排他
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare a queue")

	//绑定队列到exchange中
	err = r.channel.QueueBind(
		q.Name,
		r.Key,
		r.Exchange,
		false,
		nil,
	)

	messges, err := r.channel.Consume(
		q.Name,
		//用来区分多个消费者
		"",
		//是否自动应答
		true,
		//是否具有排他性
		false,
		//如果设置为true，表示不能将同一个connection中发送的消息传递给这个connection中的消费者
		false,
		//队列消费是否阻塞
		false,
		nil,
	)

	forever := make(chan bool)

	go func() {
		for d := range messges {
			log.Printf("Received a message:%s", d.Body)
		}
	}()

	fmt.Println("推出请按 CTRL+C\n")

	<- forever
}

func NewRabbitMQTopic(exchangeName string, routingKey string) *RabbitMQ {
	rabbitmq := NewRabbitMQ("", exchangeName, routingKey)
	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "failed to connect rabbitmq")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "failed to open a channel")
	return rabbitmq
}

func (r *RabbitMQ) PublishTopic(message string) {
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)

	r.failOnErr(err, "failed to declare an exchange")

	r.channel.Publish(
		r.Exchange,
		r.Key,
		//如果为true，根据exchange类型和route key 规则，如果无法找到符合条件的队列那么会把发送的消息返回给发送者
		false,
		//如果为true，当exchange发送消息导队列后发现队列上没有绑定消费者，则会把消息发还给发送着
		false,
		amqp.Publishing{
			ContentType:"text/plain",
			Body:[]byte(message),
		},
	)
}


//话题模式接受消息
//#:一个或多个词 *:一个词
func (r *RabbitMQ) RecieveTopic() {
	//尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		//广播类型
		"topic",
		true,
		false,
		//YES表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		false,
		nil,
	)

	//试探性的创建队列，这里的队列名称不要写
	q, err := r.channel.QueueDeclare(
		"", //随机生成队列名称
		false,
		false,
		true, //是否排他
		false,
		nil,
	)
	r.failOnErr(err, "Failed to declare a queue")

	//绑定队列到exchange中
	err = r.channel.QueueBind(
		q.Name,
		r.Key,
		r.Exchange,
		false,
		nil,
	)

	messges, err := r.channel.Consume(
		q.Name,
		//用来区分多个消费者
		"",
		//是否自动应答
		true,
		//是否具有排他性
		false,
		//如果设置为true，表示不能将同一个connection中发送的消息传递给这个connection中的消费者
		false,
		//队列消费是否阻塞
		false,
		nil,
	)

	forever := make(chan bool)

	go func() {
		for d := range messges {
			log.Printf("Received a message:%s", d.Body)
		}
	}()

	fmt.Println("推出请按 CTRL+C\n")

	<- forever
}