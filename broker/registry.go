package main

type Registry struct {
	Producers map[int64]interface{}
	Consumers map[int64]interface{}
	Topics    map[string]interface{}
	Groups    map[int64]interface{}

	ConsumerGroupRegistry map[int64]int64
	TopicGroupRegistry    map[string][]int64
	ProducerTopicRegistry map[int64][]string
}

func NewRegistry() *Registry {
	return &Registry{
		Producers: make(map[int64]interface{}),
		Consumers: make(map[int64]interface{}),
		Topics:    make(map[string]interface{}),
		Groups:    make(map[int64]interface{}),

		ConsumerGroupRegistry: make(map[int64]int64),
		TopicGroupRegistry:    make(map[string][]int64),
		ProducerTopicRegistry: make(map[int64][]string),
	}
}

func (r *Registry) AddProducer(producerId int64, topic string) {
	r.Producers[producerId] = 1
	r.ProducerTopicRegistry[producerId] = append(r.ProducerTopicRegistry[producerId], topic)
}

func (r *Registry) AddConsumer(consumerId int64, groupId int64) {
	r.Consumers[consumerId] = 1
	r.ConsumerGroupRegistry[consumerId] = groupId
}

func (r *Registry) AddGroup(groupId int64, topic string) {
	r.Groups[groupId] = 1
	r.TopicGroupRegistry[topic] = append(r.TopicGroupRegistry[topic], groupId)
}
