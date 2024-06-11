package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"google.golang.org/grpc"

	pb "notification-service-consumer/proto"
)

// MessageProcessor processes messages from different topics.
type MessageProcessor struct {
	grpcClient pb.RequestCommentServiceClient
}

func (p *MessageProcessor) ProcessMessage(topic string, message *sarama.ConsumerMessage, ctx context.Context) {
	switch topic {
	case "request":
		p.handleRequest(message, ctx)
	case "comment":
		p.handleComment(message, ctx)
	default:
		log.Printf("Unknown topic: %s\n", topic)
	}
}

func (p *MessageProcessor) handleRequest(message *sarama.ConsumerMessage, ctx context.Context) {
	var req pb.Request
	err := json.Unmarshal(message.Value, &req)
	if err != nil {
		log.Printf("Error unmarshalling request JSON: %v", err)
		return
	}

	insertReq := &pb.InsertRequest{Request: &req}
	res, err := p.grpcClient.CreateRequest(ctx, insertReq)
	if err != nil {
		log.Printf("Failed to call CreateRequest: %v", err)
		return
	}
	log.Printf("CreateRequest response: Success=%v, Message=%s", res.Success, res.Message)
}

func (p *MessageProcessor) handleComment(message *sarama.ConsumerMessage, ctx context.Context) {
	var cmt pb.Comment
	err := json.Unmarshal(message.Value, &cmt)
	if err != nil {
		log.Printf("Error unmarshalling comment JSON: %v", err)
		return
	}

	insertCmt := &pb.InsertComment{Comment: &cmt}
	res, err := p.grpcClient.CreateComment(ctx, insertCmt)
	if err != nil {
		log.Printf("Failed to call CreateComment: %v", err)
		return
	}
	log.Printf("CreateComment response: Success=%v, Message=%s", res.Success, res.Message)
}

func main() {
	// Kafka broker addresses
	brokers := []string{"localhost:29092"}

	// Kafka topics to consume
	topics := []string{"request", "comment"}

	// Create new consumer group configuration
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0 // Ensure compatibility with your Kafka version
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	// Create new consumer group
	group, err := sarama.NewConsumerGroup(brokers, "notification_group", config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer group.Close()

	// Set up a connection to the gRPC server
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()
	grpcClient := pb.NewRequestCommentServiceClient(conn)

	// Initialize the message processor with the gRPC client
	processor := &MessageProcessor{grpcClient: grpcClient}

	// Trap SIGINT and SIGTERM signals to trigger a graceful shutdown
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// Set up the consumer group handler
	handler := &consumerGroupHandler{processor: processor}

	// Context for managing lifecycle of the consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			if err := group.Consume(ctx, topics, handler); err != nil {
				log.Fatalf("Error from consumer: %v", err)
			}

			// Check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Log errors from the consumer group
	go func() {
		for err := range group.Errors() {
			log.Printf("Consumer group error: %v", err)
		}
	}()

	// Wait for a termination signal
	<-sigterm
	log.Println("Received termination signal, shutting down...")
	cancel()
}

// consumerGroupHandler handles messages for the consumer group.
type consumerGroupHandler struct {
	processor *MessageProcessor
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.processor.ProcessMessage(message.Topic, message, session.Context())
		session.MarkMessage(message, "")
	}
	return nil
}
