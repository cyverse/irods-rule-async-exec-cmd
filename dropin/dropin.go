package dropin

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// DropIn is a struct to maintain drop in config and state
type DropIn struct {
	Dir string
}

func NewDropIn(dir string) *DropIn {
	return &DropIn{
		Dir: dir,
	}
}

// MakeDropInDir makes drop in dir
func (dropin *DropIn) MakeDropInDir() error {
	dirInfo, err := os.Stat(dropin.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			// make
			mkdirErr := os.MkdirAll(dropin.Dir, 0775)
			if mkdirErr != nil {
				return fmt.Errorf("making a drop in dir (%s) error - %v", dropin.Dir, mkdirErr)
			}

			return nil
		}

		return fmt.Errorf("drop in dir (%s) error - %v", dropin.Dir, err)
	}

	if !dirInfo.IsDir() {
		return fmt.Errorf("drop in dir (%s) exist, but not a directory", dropin.Dir)
	}

	tempDirPerm := dirInfo.Mode().Perm()
	if tempDirPerm&0200 != 0200 {
		return fmt.Errorf("drop in dir (%s) exist, but does not have write permission", dropin.Dir)
	}

	return nil
}

// Drop drops a request in
func (dropin *DropIn) Drop(data []byte) error {
	// save as a file
	id := strconv.FormatInt(time.Now().UnixMicro(), 10)
	dropInFilePath := filepath.Join(dropin.Dir, id)

	return ioutil.WriteFile(dropInFilePath, data, 0o666)
}
