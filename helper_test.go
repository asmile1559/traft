package traft

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	TestDir = "./test"
)

func init() {
	if _, err := os.Stat(TestDir); os.IsNotExist(err) {
		if err := os.Mkdir(TestDir, 0755); err != nil {
			slog.Error(err.Error())
			return
		}
	}
}

func createTestFile() error {
	// 创建测试文件
	pf := [...]PFileType{PLogFile, PMetadataFile, PSnapshotFile}
	for _, pfType := range pf {
		// 创建文件名
		for i := 0; i < 10; i++ {
			fileName := filepath.Join(TestDir, time.Now().Format("2006-01-02-15:04:05")+"."+string(pfType))
			f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				slog.Error(err.Error())
				return err
			}
			_, _ = f.Write([]byte("test"))
			time.Sleep(RandomElectionTimeout() * 5)
			_ = f.Close()
		}
	}
	return nil
}

func TestGetLatestFile(t *testing.T) {
	file, err := GetLatestFile(TestDir, PLogFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("latest file: %s", file)

	file, err = GetLatestFile(TestDir, PMetadataFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("latest file: %s", file)

	file, err = GetLatestFile(TestDir, PSnapshotFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("latest file: %s", file)
}
