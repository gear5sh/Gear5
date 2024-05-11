package main

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func errorExit(msg string) {
	logrus.Error(msg)
	os.Exit(1)
}

func main() {
	viper.AutomaticEnv()

	logrus.Info("Release tool running...")
	channel := viper.GetString("CHANNEL")
	if channel != "stable" && channel != "edge" {
		errorExit(fmt.Sprintf("channel[%s] not supported", channel))
	}

	// dockerBuild := viper.GetBool("DOCKER")
	// platform := viper.GetString("PLATFORM")
	// if dockerBuild {

	// 	return
	// }
}
