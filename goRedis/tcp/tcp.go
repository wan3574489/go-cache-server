package tcp

import (
	"bufio"
	"errors"
	"fmt"
	"goRedis/Cache"
	"goRedis/cluser"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

type Server struct {
	Cache.Cache
	cluser.Node
}

func (s *Server)Listen()  {
	Listener, error := net.Listen("tcp",":12346")
	if error != nil{
		panic(error)
	}
	for {
		c,e := Listener.Accept()
		if e != nil{
			panic(e)
		}
		go s.process(c)
	}
}

type result struct {
	v []byte
	e error
}

func (s *Server) process(conn net.Conn){
	r := bufio.NewReader(conn)
	//chan的chan
	resultCh := make(chan chan *result,5000)
	defer close(resultCh)

	go reply(conn,resultCh)

	for {
		op,e := r.ReadByte()
		if e != nil{
			if e != io.EOF{
				log.Println("close connection due to error:",e)
			}
			return
		}

		if op == 'S'{
			s.set(resultCh,r)
		}else if op == 'G' {
			s.get(resultCh,r)
		}else if op == 'D'{
			s.del(resultCh,r)
		}else{
			log.Println("close connection due to invalid operation:",op)
			return
		}
		if e != nil{
			log.Println("close connection due to error",e)
			return
		}
	}
}

func reply(conn net.Conn,resultch chan chan * result)  {
	defer  conn.Close()
	for  {
		c,open := <- resultch
		if !open{
			return
		}
		r := <-c
		e := sendResponse(r.v,r.e,conn)
		if e != nil{
			log.Println("close connection due to error:",e)
			return
		}
	}
}

func (s *Server) readKey(r *bufio.Reader)(string,error)  {
	kLen,e := readLen(r)
	if e != nil{
		return "",e
	}
	k := make([]byte,kLen)
	_,e = io.ReadFull(r,k)
	if e != nil{
		return "",e
	}
	key := string(k)
	addr,ok := s.ShouldProcess(key)
	if !ok {
		return "",errors.New("redirect " + addr)
	}
	return key,nil
}

func (s *Server) readKeyAndValue(r *bufio.Reader)(string,[]byte,error)  {
	kLen,e := readLen(r)
	if e != nil{
		return "",nil,e
	}
	vLen,e := readLen(r)
	if e != nil{
		return "",nil,e
	}
	k := make([]byte,kLen)
	_,e = io.ReadFull(r,k)
	if e != nil{
		return "",nil,e
	}
	key := string(k)
	addr,ok := s.ShouldProcess(key)
	if !ok {
		return "",nil,errors.New("redirect " + addr)
	}

	v := make([]byte,vLen)
	_,e = io.ReadFull(r,v)
	if e != nil{
		return "",nil,e
	}
	return key,v,nil
}

func (s *Server)get(resultCh chan chan *result,r *bufio.Reader)  {
	c := make(chan *result)
	//写入一个chan *result
	resultCh <- c
	k ,e := s.readKey(r)

	if e != nil	{
		c <- &result{nil,e}
		return
	}
	go func() {
		v ,e := s.Get(k)
		c <- &result{v,e}
	}()

}

func (s *Server)set(resultCh chan chan *result,reader *bufio.Reader)  {
	c := make(chan *result)
	resultCh <- c
	k,v,e := s.readKeyAndValue(reader)
	if e != nil{
		c <- &result{nil,e}
		return
	}

	go func() {
		 e	:= s.Set(k,v)
		 c <- &result{nil,e}
	}()
}

func (s *Server)del(resultCh chan chan *result,reader *bufio.Reader)   {

	c := make(chan *result)
	resultCh <- c
	k,e := s.readKey(reader)

	if e != nil{
		c <- &result{nil,e}
		return
	}

	go func() {
		e := s.Del(k)
		c <- &result{nil,e}
	}()
}

func readLen(r *bufio.Reader) (int, error) {
	tmp,e := r.ReadString(' ')
	if e != nil{
		return 0,e
	}
	l,e := strconv.Atoi(strings.TrimSpace(tmp))
	if e != nil{
		return 0,e
	}
	return l,nil
}

func sendResponse(value []byte,err error,conn net.Conn) error{
	if err != nil{
		errString := err.Error()
		tmp := fmt.Sprintf("-%d ",len(errString))+errString
		_,e := conn.Write([]byte(tmp))
		return e
	}
	vLen := fmt.Sprintf("%d ",len(value))
	_,e := conn.Write(append([]byte(vLen),value...))
	return e
}

func New(c Cache.Cache,n cluser.Node)*Server  {
	return &Server{c,n}
}