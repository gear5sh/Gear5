package main

import "github.com/piyushsingariya/syndicate"

func main() {
	syndicate.RegisterDriver(&GoogleSheets{})
}
