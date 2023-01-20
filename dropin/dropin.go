package dropin

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// DropIn is a struct to maintain drop in config and state
type DropIn struct {
	Dir       string
	FailedDir string
}

func NewDropIn(dir string) *DropIn {
	return &DropIn{
		Dir:       dir,
		FailedDir: path.Join(dir, "failed"),
	}
}

// MakeDropInDir makes drop in dir
func (dropin *DropIn) MakeDropInDir() error {
	dropinDirInfo, err := os.Stat(dropin.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			// make
			mkdirErr := os.MkdirAll(dropin.Dir, 0775)
			if mkdirErr != nil {
				return fmt.Errorf("making a drop in dir (%s) error - %v", dropin.Dir, mkdirErr)
			}

			// okay
		} else {
			return fmt.Errorf("drop in dir (%s) error - %v", dropin.Dir, err)
		}
	}

	if dropinDirInfo != nil {
		if !dropinDirInfo.IsDir() {
			return fmt.Errorf("drop in dir (%s) exist, but not a directory", dropin.Dir)
		}

		dropinDirPerm := dropinDirInfo.Mode().Perm()
		if dropinDirPerm&0200 != 0200 {
			return fmt.Errorf("drop in dir (%s) exist, but does not have write permission", dropin.Dir)
		}
	}

	failedDropinDirInfo, err := os.Stat(dropin.FailedDir)
	if err != nil {
		if os.IsNotExist(err) {
			// make
			mkdirErr := os.MkdirAll(dropin.FailedDir, 0775)
			if mkdirErr != nil {
				return fmt.Errorf("making a failed drop in dir (%s) error - %v", dropin.FailedDir, mkdirErr)
			}

			// okay
		} else {
			return fmt.Errorf("failed drop in dir (%s) error - %v", dropin.FailedDir, err)
		}
	}

	if failedDropinDirInfo != nil {
		if !failedDropinDirInfo.IsDir() {
			return fmt.Errorf("failed drop in dir (%s) exist, but not a directory", dropin.FailedDir)
		}

		failedDropinDirPerm := failedDropinDirInfo.Mode().Perm()
		if failedDropinDirPerm&0200 != 0200 {
			return fmt.Errorf("failed drop in dir (%s) exist, but does not have write permission", dropin.FailedDir)
		}
	}

	return nil
}

// Drop drops a request in
func (dropin *DropIn) Drop(item DropInItem) error {
	// save as a file
	id := strconv.FormatInt(time.Now().UnixMicro(), 10)
	pid := os.Getpid()
	filename := fmt.Sprintf("%s-%d", id, pid)
	dropInFilePath := filepath.Join(dropin.Dir, filename)

	return item.SaveToFile(dropInFilePath)
}

// Scrape finds all drop-ins
func (dropin *DropIn) Scrape() ([]DropInItem, error) {
	files, err := os.ReadDir(dropin.Dir)
	if err != nil {
		return nil, err
	}

	items := []DropInItem{}
	for _, file := range files {
		if !file.IsDir() {
			fullpath := filepath.Join(dropin.Dir, file.Name())
			item, reqErr := NewDropInRequestFromFile(fullpath)
			if reqErr != nil {
				err = reqErr
				failDirFile := filepath.Join(dropin.FailedDir, filepath.Base(fullpath))
				os.Rename(fullpath, failDirFile)
				continue
			}

			items = append(items, item)
		}
	}

	// sort by file name
	sort.SliceStable(items[:], func(i int, j int) bool {
		basei := filepath.Base(items[i].GetItemFilePath())
		basej := filepath.Base(items[j].GetItemFilePath())
		return basei < basej
	})

	return items, err
}

// MarkFailed sets a drip-in failed
func (dropin *DropIn) MarkFailed(item DropInItem) error {
	fullpath := item.GetItemFilePath()
	if len(fullpath) > 0 {
		failDirFile := filepath.Join(dropin.FailedDir, filepath.Base(fullpath))
		return os.Rename(fullpath, failDirFile)
	}
	return nil
}

// MarkSuccess sets a drip-in success
func (dropin *DropIn) MarkSuccess(item DropInItem) error {
	fullpath := item.GetItemFilePath()
	if len(fullpath) > 0 {
		return os.Remove(fullpath)
	}
	return nil
}
