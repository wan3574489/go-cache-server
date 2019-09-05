package main

import (
	"flag"
	"goRedis/Cache"
	"goRedis/cluser"
	"goRedis/http"
	"goRedis/tcp"
	"log"
)

func main() {

	typ := flag.String("type","inmemory","cache type")
	node := flag.String("node","127.0.0.1","node address")
	clus := flag.String("cluster","","cluster address")
	port := flag.Int("port",7946,"node port")
	flag.Parse()

	log.Println("type is ",*typ)
	log.Println("node is ", *node)
	log.Println("cluster is ",*clus)

	cache := Cache.New(*typ)
	n,e := cluser.New(*node,*clus,*port)
	if e != nil{
		panic(e)
	}
	go tcp.New(cache,n).Listen()
	server := http.New(cache,n)
	server.Listen()
}
