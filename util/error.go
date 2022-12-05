package util

import "fmt"

func FormatErrorMessage(baseErrorMessage string, logOpts []string) string {
	if logOpts == nil || len(logOpts) == 0 {
		return baseErrorMessage
	}

	if len(logOpts) == 1 {
		return fmt.Sprintf(baseErrorMessage+" in collection, %s", logOpts[0])
	}

	return fmt.Sprintf(baseErrorMessage+" for key, %s => %s", logOpts[0], logOpts[1])
}
