package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const ConsumerId = "kafka-consumer-tools"

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

// ConsumerGroupHandler 实现 sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	filter  string
	jsonOut string
	file    *os.File
}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}
		val := cleanString(string(msg.Value))
		if h.filter != "" && !strings.Contains(val, h.filter) {
			continue
		}

		var rec MessageRecord
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

		if h.file != nil {
			data, _ := json.MarshalIndent(rec, "", "  ")
			h.file.Write(data)
			h.file.Write([]byte("\n"))

			count := atomic.AddUint64(&msgCount, 1)
			fmt.Printf("已写入第 %d 条消息\n", count)
		} else {
			fmt.Printf("分区:%d offset:%d key:%s value:%v\n",
				msg.Partition, msg.Offset, string(msg.Key), rec.Value)
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	// 命令行参数
	broker := flag.String("broker", "127.0.0.1:9092", "Kafka broker 地址，支持多个以逗号分隔 (例如: broker1:9092,broker2:9092)")
	topic := flag.String("topic", "", "Kafka 主题 (必填)")
	filter := flag.String("filter", "", "消息过滤关键字 (可选，类似 grep)")
	username := flag.String("user", "", "用户名 (可选)")
	password := flag.String("pass", "", "密码 (可选)")
	offsetOldest := flag.Bool("from-beginning", false, "是否从头开始消费 (默认最新)")
	jsonOut := flag.String("json-out", "", "将消息以 JSON 格式写入文件 (可选，带缩进)")

	flag.Parse()

	if *topic == "" {
		fmt.Println("必须指定 -topic 参数")
		flag.Usage()
		os.Exit(1)
	}

	brokers := strings.Split(*broker, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i]) // 去除可能的空格
	}

	// Kafka 配置
	config := sarama.NewConfig()
	config.Version = sarama.MaxVersion
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	if *offsetOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

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
	consumerId := ConsumerId + time.Now().Format("20060102150405")
	group, err := sarama.NewConsumerGroup(brokers, consumerId, config)
	go func() {
		for err := range group.Errors() {
			fmt.Println("Kafka 错误:", err)
		}
	}()
	if err != nil {
		fmt.Println("创建 ConsumerGroup 失败:", err)
		os.Exit(1)
	}
	defer group.Close()

	handler := &ConsumerGroupHandler{
		filter:  *filter,
		jsonOut: *jsonOut,
		file:    file,
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	topics := []string{*topic}
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			// 如果 context 已取消，退出循环
			if ctx.Err() != nil {
				group.Close()
				return
			}

			err := group.Consume(ctx, topics, handler)
			if err != nil {
				fmt.Println("消费错误:", err)
				// 如果 group 已关闭，也退出
				if strings.Contains(err.Error(), "closed") {
					return
				}
			}
		}
	}()

	<-signals
	cancel() // ✅ 通知 goroutine 停止消费
	fmt.Println("收到退出信号，正在关闭...")
	time.Sleep(time.Second * 3)
	// ✅ 删除当前 Consumer Group
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		fmt.Println("创建 Admin 客户端失败:", err)
	} else {
		err = admin.DeleteConsumerGroup(consumerId)
		if err != nil {
			fmt.Printf("删除消费者组 %s 失败: %v\n", consumerId, err)
		} else {
			fmt.Printf("已成功删除消费者组: %s\n", consumerId)
		}
		admin.Close()
	}

	fmt.Println("退出完成，总共写入消息:", msgCount)
}
