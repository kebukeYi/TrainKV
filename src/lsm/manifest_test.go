package lsm

import (
	"os"
	"path/filepath"
	"testing"
	"trainKv/common"
	"trainKv/utils"
)

func TestOpenManifestFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Test case 1: File does not exist, should create a new file and manifest
	t.Run("FileDoesNotExist", func(t *testing.T) {
		opt := &utils.FileOptions{Dir: tempDir}
		manifestFile, err := OpenManifestFile(opt)
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

	// Test case 2: File exists, should open the existing file and manifest
	t.Run("FileExists", func(t *testing.T) {
		opt := &utils.FileOptions{Dir: tempDir}
		manifestFile, _ := OpenManifestFile(opt)

		// Create a dummy file with some content
		content := []byte("dummy content")
		err = os.WriteFile(filepath.Join(tempDir, common.ManifestFilename), content, common.DefaultFileMode)
		if err != nil {
			t.Fatalf("Failed to create dummy file: %v", err)
		}

		// Open the existing file
		manifestFile, err = OpenManifestFile(opt)
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
