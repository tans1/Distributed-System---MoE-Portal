package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)


type DistributedLock struct {
	Key        string
	Value      string
	LeaseID    clientv3.LeaseID
	etcdClient *clientv3.Client
}

type Server struct{
	Address *url.URL
	Latitude float64
	Longitude float64
	
}

func (dl *DistributedLock) Lock(ctx context.Context, ttl int64) error {


	lease, err := dl.etcdClient.Grant(ctx, ttl)

	if err != nil {
		return err
	}

	resp, err := dl.etcdClient.Txn(ctx).
	If(clientv3.Compare(clientv3.Version(dl.Key), "=", 0)).
		Then(clientv3.OpPut(dl.Key, dl.Value, clientv3.WithLease(lease.ID))).
		Commit()

	if err != nil {
		fmt.Printf("Lock already taken")
		return err
	}

	if !resp.Succeeded{
		return errors.New("error acquring lock")
	}

	dl.LeaseID = lease.ID
	log.Printf("Lock acquired: %s", dl.Key)
	return nil
}

func (dl *DistributedLock) Unlock(ctx context.Context) error {
	_, err := dl.etcdClient.Delete(ctx, dl.Key)
	if err != nil {
		return err
	}

	_, err = dl.etcdClient.Revoke(ctx, dl.LeaseID)
	if err != nil {
		return err
	}

	log.Printf("Lock released: %s", dl.Key)
	return nil
}


type LoadBalancer struct {
	servers []Server
	mutex   sync.Mutex
}

type Location struct{
	Latitude float64
	Longitude float64
}

type Tuple struct{
	distance float64
	server Server
}
func degToRad(deg float64) float64 {
	return deg * (math.Pi / 180)
}

func Harvsine(serverLocation Location,requestLocation Location)float64{
	R := 6371.0
	lat1 := degToRad(serverLocation.Latitude)
	lat2 := degToRad(requestLocation.Latitude)
	lon1 := degToRad(serverLocation.Longitude)
	lon2 := degToRad(requestLocation.Longitude)
	dlat := lat2 - lat1
	dlon := lon2 - lon1

	// Haversine formula
	a := math.Pow(math.Sin(dlat/2), 2) + math.Cos(lat1)*math.Cos(lat2)*math.Pow(math.Sin(dlon/2), 2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	// Distance
	distance := R * c

	return distance

}

func (lb *LoadBalancer) DistanceCalculator(requestLocation Location)[]Tuple{
	
	distanceServerMap := []Tuple{}
	
	for _,server := range lb.servers{
		serverLocation := Location{server.Latitude,server.Longitude}
		distance := Harvsine(serverLocation,requestLocation)
		distanceServerMap = append(distanceServerMap, Tuple{server: server,distance: distance})
	}

	sort.Slice(distanceServerMap,func(i, j int) bool {
		return distanceServerMap[i].distance < distanceServerMap[j].distance
	})

	return distanceServerMap
}

func (lb *LoadBalancer) nextServer(requestLocation Location) *url.URL {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()
	serverDistanceMap := lb.DistanceCalculator(requestLocation)

	for _,val := range serverDistanceMap{
		address := val.server.Address
		running,_ := lb.checkHealth(address.String())
		if !running{
			return address
		}
	}
	// To be implemented Here if all severs fail
	return &url.URL{}
}

func (lb *LoadBalancer) handleRequest(w http.ResponseWriter, r *http.Request) {
	fmt.Println("request is coming")
	lat,_ := strconv.ParseFloat(r.Header.Get("Latitude"), 64)
	long,_ := strconv.ParseFloat(r.Header.Get("Longitude"), 64)
	requestLocation := Location {
		Latitude : lat,
		Longitude: long,
	}
	server := lb.nextServer(requestLocation)
	// Reverse proxy to the selected backend server
	proxy := httputil.NewSingleHostReverseProxy(server)
	proxy.ServeHTTP(w, r)
}

func (lb *LoadBalancer) start(dl DistributedLock,ctx context.Context){

	for {
		
		// Acquire the lock
		err := dl.Lock(ctx, 20) // Set TTL to 10 seconds
		if err != nil {
			fmt.Println("unable to acuire the lock because it is being used")
			continue
		}

		activeServerLocation := dl.Value
		isActive, _ := lb.checkHealth(activeServerLocation)
		
		if !isActive{
			// If active server returns False, start listening at the active port
			lb.startListening(activeServerLocation)
			break
		} 

		time.Sleep(time.Second * 10)
		errs := dl.Unlock(ctx) 
		if errs != nil {
			fmt.Println("Failed to Unlock")
		}
	}
}
func (lb *LoadBalancer) checkHealth(address string)(bool,error){
	conn, err := net.DialTimeout("tcp", address, 1*time.Second)
	if err != nil {
		return false,err
	}

	conn.Close()
	return true,err
}
func (lb *LoadBalancer) startListening(address string){
	http.ListenAndServe(address, nil)
}

func main() {
	lb := &LoadBalancer{
		servers: []Server{
			Server{
				Address:   parseURL("http://localhost:3030"),
				Latitude:  10.5,
				Longitude: 20.6,
			},
			Server{
				Address:   parseURL("http://localhost:3031"),
				Latitude:  70.5,
				Longitude: 46.5,
			},
		},
	
	}
	http.HandleFunc("/", lb.handleRequest)

	endpoints := []string{"localhost:2379"}

	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}

	client, err := clientv3.New(cfg)
	if err != nil {
		fmt.Printf("Error connecting to etcd: %v", err)
		os.Exit(1)
	}

	defer client.Close()

	ctx := context.Background()
	lockKey := "active-sever-address"
	lockValue := ":8080"

	dl := DistributedLock{
		Key:        lockKey,
		Value:      lockValue,
		etcdClient: client,
	}	
	
	lb.start(dl,ctx)

}

func parseURL(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return u
}
