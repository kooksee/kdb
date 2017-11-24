package main

import (
	"log"

	"kdb"
)

func main() {
	// New bolt database
	db, err := kdb.New("bolt.db")
	if err != nil {
		log.Fatalf("Could not create database! %s", err)
	}
	defer db.Close()

	// Create a list named "greetings"
	list, err := kdb.NewList(db, "greetings")
	if err != nil {
		log.Fatalf("Could not create a list! %s", err)
	}

	// Add "hello" to the list
	if err := list.Add("hello"); err != nil {
		log.Fatalf("Could not add an item to the list! %s", err)
	}

	// Get the last item of the list
	if item, err := list.GetLast(); err != nil {
		log.Fatalf("Could not fetch the last item from the list! %s", err)
	} else {
		log.Println("The value of the stored item is:", item)
	}

	// Remove the list
	if err := list.Remove(); err != nil {
		log.Fatalf("Could not remove the list! %s", err)
	}
}
