package discover

import (
	"github.com/go-kit/kit/sd/consul"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"log"
	"strconv"
	"sync"
)

type KitDiscoverClient struct {
	Host         string
	Port         int
	client       consul.Client
	config       *api.Config
	mutex        sync.Mutex
	instancesMap sync.Map
}

func (consulClient *KitDiscoverClient) DiscoveryService(serviceName string, logger log.Logger) []interface{} {
	// 该服务是否已监控并缓存
	instanceList, ok := consulClient.instancesMap.Load(serviceName)
	if ok {
		return instanceList.([]interface{})
	}
	// 申请锁
	consulClient.mutex.Lock()
	defer consulClient.mutex.Unlock()
	// 再次检查是否监控
	instanceList, ok = consulClient.instancesMap.Load(serviceName)
	if ok {
		return instanceList.([]interface{})
	} else {
		// 注册监控
		go func() {
			// 使用 consul 服务实例监控来监控某个服务名的服务实例列表变化
			params := make(map[string]interface{})
			params["type"] = "service"
			params["service"] = serviceName
			plan, _ := watch.Parse(params)
			plan.Handler = func(u uint64, i interface{}) {
				if i == nil {
					return
				}
				v, ok := i.([]*api.ServiceEntry)
				if !ok {
					return // 数据异常，忽略
				}
				if len(v) == 0 {
					consulClient.instancesMap.Store(serviceName, []interface{}{})
				}
				var healthServices []interface{}
				for _, service := range v {
					if service.Checks.AggregatedStatus() == api.HealthPassing {
						healthServices = append(healthServices, service.Service)
					}
				}
				consulClient.instancesMap.Store(serviceName, healthServices)
				defer plan.Stop()
				plan.Run(consulClient.config.Address)
			}
		}()
	}
	// 根据服务名请求服务实例列表
	entries, _, err := consulClient.client.Service(serviceName, "", false, nil)
	if err != nil {
		consulClient.instancesMap.Store(serviceName, []interface{}{})
		log.Printf("Discover Service Error: %s\n", err.Error())
		return nil
	}
	instances := make([]interface{}, len(entries))
	for i := 0; i < len(instances); i++ {
		instances[i] = entries[i].Service
	}
	return instances
}

func (consulClient *KitDiscoverClient) DeRegister(instanceId string, logger log.Logger) bool {
	// 构建包含服务实例Id的元数据结构体
	serviceRegistration := &api.AgentServiceRegistration{ID: instanceId}

	// 向 Consul 发送注销请求
	err := consulClient.client.Deregister(serviceRegistration)
	if err != nil {
		log.Printf("DeRegister Service Error: %v", err)
		return false
	}
	log.Println("DeRegister Service Success!")
	return true
}

func (consulClient *KitDiscoverClient) Register(serviceName, instanceId, healthCheckUrl, instanceHost string, instancePort int, meta map[string]string, logger log.Logger) bool {
	// 1.构建服务实例元数据
	serviceRegistration := &api.AgentServiceRegistration{
		ID:      instanceId,
		Name:    serviceName,
		Port:    instancePort,
		Address: instanceHost,
		Meta:    meta,
		Check: &api.AgentServiceCheck{
			Interval:                       "15s",
			HTTP:                           "http://" + instanceHost + ":" + strconv.Itoa(instancePort) + healthCheckUrl,
			DeregisterCriticalServiceAfter: "30s",
		},
	}

	// 2.发送服务实例到 Consul 中
	err := consulClient.client.Register(serviceRegistration)
	if err != nil {
		log.Printf("Register Service Error: %v", err)
		return false
	}
	log.Println("Register Service Success!")
	return true
}
