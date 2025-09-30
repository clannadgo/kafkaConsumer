package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/IBM/sarama"
)

// MessageRecord 用于输出 JSON 文件
type MessageRecord struct {
	Partition int32       `json:"partition"`
	Offset    int64       `json:"offset"`
	Key       string      `json:"key,omitempty"`
	Value     interface{} `json:"value"` // 可能是 string 或 JSON 对象
}

var msgCount uint64 // 消息计数器

// cleanString 去掉 ASCII 控制字符
func cleanString(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, s)
}

func main() {
	// 命令行参数
	ip := flag.String("ip", "127.0.0.1", "Kafka broker IP 地址")
	port := flag.String("port", "9092", "Kafka broker 端口")
	topic := flag.String("topic", "", "Kafka 主题 (必填)")
	filter := flag.String("filter", "", "消息过滤关键字 (可选，类似 grep)")
	username := flag.String("user", "", "用户名 (可选)")
	password := flag.String("pass", "", "密码 (可选)")
	offsetOldest := flag.Bool("from-beginning", false, "是否从头开始消费 (默认最新)")
	jsonOut := flag.String("json-out", "", "将消息以 JSON 格式写入文件 (可选，带缩进)")

	// 自定义 Usage
	flag.Usage = func() {
		_, _ = fmt.Fprintf(os.Stderr, "用法: %s [参数]\n\n参数说明:\n", os.Args[0])
		_, _ = fmt.Fprintf(os.Stderr, "  -ip string\n\tKafka broker IP 地址 (默认 \"127.0.0.1\")\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -port string\n\tKafka broker 端口 (默认 \"9092\")\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -topic string\n\tKafka 主题 (必填)\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -filter string\n\t消息过滤关键字 (可选，类似 grep)\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -user string\n\t用户名 (可选)\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -pass string\n\t密码 (可选)\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -from-beginning\n\t是否从头开始消费 (默认最新)\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -json-out string\n\t将消息以 JSON 格式写入文件 (可选，带缩进)\n")
	}
	flag.Parse()

	if *topic == "" {
		fmt.Println("必须指定 -topic 参数")
		flag.Usage()
		os.Exit(1)
	}

	broker := fmt.Sprintf("%s:%s", *ip, *port)

	// Kafka 配置
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.MaxVersion

	if *username != "" && *password != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = *username
		config.Net.SASL.Password = *password
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}

	// 打开 JSON 文件（追加模式）
	var file *os.File
	var err error
	if *jsonOut != "" {
		file, err = os.OpenFile(*jsonOut, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Println("无法打开文件:", err)
			os.Exit(1)
		}
		defer file.Close()
		fmt.Printf("消息将以 JSON 格式写入文件: %s\n", *jsonOut)
	}

	// 创建 Kafka 消费者
	consumer, err := sarama.NewConsumer([]string{broker}, config)
	if err != nil {
		fmt.Println("创建消费者失败:", err)
		os.Exit(1)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(*topic)
	if err != nil {
		fmt.Println("获取分区失败:", err)
		os.Exit(1)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan struct{})

	var wg sync.WaitGroup
	offset := sarama.OffsetNewest
	if *offsetOldest {
		offset = sarama.OffsetOldest
	}

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(*topic, partition, offset)
		if err != nil {
			fmt.Printf("订阅分区 %d 失败: %v\n", partition, err)
			continue
		}

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer pc.Close()
			defer wg.Done()
			for {
				select {
				case msg := <-pc.Messages():
					if msg == nil {
						continue
					}
					val := cleanString(string(msg.Value))
					if *filter != "" && !strings.Contains(val, *filter) {
						continue
					}

					var rec MessageRecord

					// 尝试解析 value 为 JSON
					var parsed interface{}
					if err := json.Unmarshal([]byte(val), &parsed); err == nil {
						rec = MessageRecord{
							Partition: msg.Partition,
							Offset:    msg.Offset,
							Key:       string(msg.Key),
							Value:     parsed,
						}
					} else {
						rec = MessageRecord{
							Partition: msg.Partition,
							Offset:    msg.Offset,
							Key:       string(msg.Key),
							Value:     val,
						}
					}

					if *jsonOut != "" {
						data, _ := json.MarshalIndent(rec, "", "  ")
						file.Write(data)
						file.Write([]byte("\n"))

						count := atomic.AddUint64(&msgCount, 1)
						fmt.Printf("已写入第 %d 条消息\n", count)
					} else {
						fmt.Printf("分区:%d offset:%d key:%s value:%v\n",
							msg.Partition, msg.Offset, string(msg.Key), rec.Value)
					}

				case err := <-pc.Errors():
					if err != nil {
						fmt.Println("消费错误:", err)
					}
				case <-done:
					return
				}
			}
		}(pc)
	}

	<-signals
	fmt.Println("收到退出信号，正在关闭...")
	close(done)
	wg.Wait()
	fmt.Println("退出完成，总共写入消息:", msgCount)
}
