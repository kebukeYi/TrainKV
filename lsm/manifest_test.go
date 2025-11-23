package lsm

import (
	"github.com/kebukeYi/TrainKV/utils"
	"testing"
)

var manifestTestPath = "/usr/golanddata/trainkv/manifest"

func TestOpenManifestFile(t *testing.T) {
	clearDir(manifestTestPath)
	tempDir := manifestTestPath

	t.Run("FileDoesNotExist", func(t *testing.T) {
		opt := &utils.FileOptions{Dir: tempDir}
		manifestFile, err := OpenManifestFile(opt)
		defer manifestFile.Close()
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if manifestFile == nil {
			t.Error("Expected non-nil ManifestFile")
		}
		if manifestFile.f == nil {
			t.Error("Expected non-nil file")
		}
		if manifestFile.manifest == nil {
			t.Error("Expected non-nil manifest")
		}
	})

	t.Run("FileExists", func(t *testing.T) {
		opt := &utils.FileOptions{Dir: tempDir}
		// Open the existing file
		manifestFile, err := OpenManifestFile(opt)
		defer manifestFile.Close()
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
		if manifestFile == nil {
			t.Error("Expected non-nil ManifestFile")
		}
		if manifestFile.f == nil {
			t.Error("Expected non-nil file")
		}
		if manifestFile.manifest == nil {
			t.Error("Expected non-nil manifest")
		}
	})
}
