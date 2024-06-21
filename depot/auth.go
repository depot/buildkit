package depot

import "os"

// BearerFromEnv returns the bearer token from the environment.
// This is used to auth to the API.
func BearerFromEnv() string {
	token := os.Getenv("DEPOT_BUILDKIT_TOKEN")
	if token != "" {
		return "Bearer " + token
	}
	return ""
}
