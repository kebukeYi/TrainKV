package lsm

import (
	"testing"
	"trainKv/utils"
)

var manifestTestPath = "/usr/projects_gen_data/goprogendata/trainkvdata/test/manifest"

func TestOpenManifestFile(t *testing.T) {
	clearDir(manifestTestPath)
	tempDir := manifestTestPath

	// Test case 1: File does not exist, should create a new file and manifest.
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

	// Test case 2: File exists, should open the existing file and manifest.
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
