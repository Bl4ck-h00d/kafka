package main

type Registry struct {
	Producers map[string]interface{}
	Consumers map[string]interface{}
	Topics    map[string]interface{}
	Groups    map[string]interface{}

	ConsumerGroupRegistry map[string]string
	TopicGroupRegistry    map[string][]string
	ProducerTopicRegistry map[string][]string
	OffsetRegistry        map[string]string // "topic/offset" <> consumerId
}

func NewRegistry() *Registry {
	return &Registry{
		Producers: make(map[string]interface{}),
		Consumers: make(map[string]interface{}),
		Topics:    make(map[string]interface{}),
		Groups:    make(map[string]interface{}),

		ConsumerGroupRegistry: make(map[string]string),
		TopicGroupRegistry:    make(map[string][]string),
		ProducerTopicRegistry: make(map[string][]string),
	}
}

func (r *Registry) AddProducer(producerId string, topic string) {
	r.Producers[producerId] = struct{}{}
	r.ProducerTopicRegistry[producerId] = append(r.ProducerTopicRegistry[producerId], topic)
	r.Topics[topic] = struct{}{}
}

func (r *Registry) AddConsumer(consumerId string, groupId string) {
	r.Consumers[consumerId] = struct{}{}
	r.ConsumerGroupRegistry[consumerId] = groupId
}

func (r *Registry) AddGroup(groupId string, topic string) {
	r.Groups[groupId] = struct{}{}
	r.TopicGroupRegistry[topic] = append(r.TopicGroupRegistry[topic], groupId)
	r.Topics[topic] = struct{}{}
}
