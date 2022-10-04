package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	c := make(chan int)

	for i := 0; i < 100; i++ {
		go work(c)
	}
	for {
		v := <-c
		fmt.Println(v)
	}
}

func work(c chan int) {
	for {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		c <- rand.Int()
	}
}
