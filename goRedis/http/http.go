package http

import (
	"bytes"
	"encoding/json"
	"goRedis/Cache"
	"goRedis/cluser"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type Server struct {
	Cache.Cache
	cluser.Node
}

type cacheHandler struct {
	*Server
}

type statusHandler struct {
	*Server
}
type clusterHandler struct {
	*Server
}
type rebalanceHandler struct {
	*Server
}

func (s *Server) Listen()  {
	http.Handle("/cache/",s.cacheHandler())
	http.Handle("/status",s.statusHandler())
	http.Handle("/cluster",s.clusterHandler())
	http.Handle("/rebalance",s.rebalanceHandler())
	http.ListenAndServe(":12345",nil)
}

func New(c Cache.Cache,n cluser.Node) *Server{
	return &Server{
		c,
		n,
	}
}

func (h *cacheHandler)ServeHTTP(w http.ResponseWriter,r *http.Request)  {
	key :=strings.Split(r.URL.EscapedPath(),"/")[2]
	if len(key) == 0{
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	m := r.Method
	//PUT 写入
	if m == http.MethodPut{
		b,_ := ioutil.ReadAll(r.Body)
		if len(b) != 0{
			e := h.Set(key,b)
			if  e != nil{
				log.Println(e)
				w.WriteHeader(http.StatusInternalServerError)
			}
		}
		return
	}
	//获取
	if m == http.MethodGet{
		v,e := h.Get(key)
		if e != nil {
			log.Println(e)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if len(v) == 0{
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Write(v)
		return
	}
	//删除
	if m == http.MethodDelete{
		e := h.Del(key)
		if e != nil{
			log.Println(e)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (h *statusHandler)ServeHTTP(w http.ResponseWriter,r *http.Request)  {
	if r.Method != http.MethodGet{
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	b,e := json.Marshal(h.GetStat())
	if e != nil{
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

func (h *clusterHandler)ServeHTTP(w http.ResponseWriter,r *http.Request){
	if r.Method != http.MethodGet{
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	m := h.Members()
	b ,e := json.Marshal(m)
	if e != nil{
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

func (h *rebalanceHandler)ServeHTTP(w http.ResponseWriter,r *http.Request){
	if r.Method != http.MethodPost{
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	go h.rebalance()
}

func (h *rebalanceHandler) rebalance()  {
	s :=h.NewScanner()
	defer s.Close()

	c := &http.Client{}
	for s.Scan(){
		k := s.Key()
		n,ok := h.ShouldProcess(k)
		if !ok{
			r,_ := http.NewRequest(http.MethodPut,"http://"+n+":12345/cache/"+k,bytes.NewReader(s.Value()))
			c.Do(r)
			h.Del(k)
		}
	}
}

func (s *Server)cacheHandler() http.Handler  {
	return &cacheHandler{s}
}

func (s *Server)statusHandler() http.Handler{
	return &statusHandler{s}
}
func (s *Server)clusterHandler() http.Handler{
	return &clusterHandler{s}
}

func (s *Server)rebalanceHandler() http.Handler{
	return &rebalanceHandler{s}
}