package env

import (
	"log"
	"os"
)

func GetOrDefault(name string, defaultValue string) (value string) {
	value = os.Getenv(name)
	if value == "" {
		value = defaultValue
		log.Printf("Read env var %s: not found; defaulting to '%s'\n", name, value)
	} else {
		log.Printf("Read env var %s: using '%s'\n", name, value)
	}
	return
}

func GetOrSupply(name string, defaultValueSupplier func() string) (value string) {
	value = os.Getenv(name)
	if value == "" {
		value = defaultValueSupplier()
		log.Printf("Read env var %s: not found; defaulting to '%s'\n", name, value)
	} else {
		log.Printf("Read env var %s: using '%s'\n", name, value)
	}
	return
}

func GetOrFail(name string) (value string) {
	value = os.Getenv(name)
	if value == "" {
		log.Fatalf("Read env var %s: required but not found; failing\n", name)
	} else {
		log.Printf("Read env var %s: using '%s'\n", name, value)
	}
	return
}

func GetSecretOrFail(name string) (value string) {
	value = os.Getenv(name)
	if value == "" {
		log.Fatalf("Read env var %s: required but not found; failing\n", name)
	} else {
		log.Printf("Read env var %s: using <secret>\n", name)
	}
	return
}
