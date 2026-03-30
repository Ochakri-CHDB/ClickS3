package main

import "fmt"

const (
	Version   = "0.1.0"
	BuildName = "clicks3"
)

func PrintVersion() {
	fmt.Printf("%s v%s\n", BuildName, Version)
}
