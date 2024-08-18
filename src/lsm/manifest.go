package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"trainKv/common"
	"trainKv/model"
	"trainKv/pb"
)

type ManifestFile struct {
	opt                      *model.FileOptions
	f                        *os.File
	mux                      sync.Mutex
	deletionRewriteThreshold int
	manifest                 *Manifest
}

type Manifest struct {
	Levels    []LevelManifest
	Tables    map[uint64]TableManifest
	Creations int
	Deletions int
}

type LevelManifest struct {
	TableIds map[uint64]struct{}
}

type TableManifest struct {
	ID       uint64
	LevelID  uint8
	CheckSum []byte
}

type TableMeta struct {
	ID       uint64
	Checksum []byte
}

func OpenManifestFile(opt *model.FileOptions) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, common.ManifestFilename)
	mf := &ManifestFile{
		opt:                      opt,
		mux:                      sync.Mutex{},
		deletionRewriteThreshold: 0,
		f:                        nil,
		manifest:                 nil,
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if os.IsNotExist(err) {
			fp, err := os.OpenFile(path, common.DefaultFileFlag, common.DefaultFileMode)
			if err != nil {
				return nil, err
			}
			mf.f = fp
			mf.manifest = NewManifest()
			return mf, nil
		}
		return nil, err
	}
	manifest, truncOffset, err := ReplyManifestFile(file)
	if err != nil {
		_ = file.Close()
		return mf, err
	}
	err = file.Truncate(truncOffset)
	if err != nil {
		_ = file.Close()
		return mf, err
	}
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return mf, err
	}
	mf.f = file
	mf.manifest = manifest
	return mf, nil
}

func ReplyManifestFile(file *os.File) (m *Manifest, truncOffset int64, err error) {
	manifestFileHeaderBuf := make([]byte, common.ManifestFileHeaderLen)
	readHeaderNum, err := io.ReadFull(file, manifestFileHeaderBuf)
	if err != nil {
		return &Manifest{}, 0, common.ErrBadReadMagic
	}
	if !bytes.Equal(manifestFileHeaderBuf[:4], common.MagicText[:]) {
		return &Manifest{}, 0, common.ErrBadMagic
	}
	version := binary.BigEndian.Uint32(manifestFileHeaderBuf[4:8])
	if version != common.MagicVersion {
		return &Manifest{}, 0, fmt.Errorf("manifest has unsupported version: %d (we support %d)",
			version, common.MagicVersion)
	}
	manifest := NewManifest()
	var offset = readHeaderNum
	for {
		crcBuf := make([]byte, common.ManifestFileCrcLen)
		readCrcNum, err := io.ReadFull(file, crcBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, common.ErrBadReadCRC
		}
		dataLength := binary.BigEndian.Uint32(crcBuf[:4])
		dataBuf := make([]byte, dataLength)
		readDataNum, err := io.ReadFull(file, dataBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}

		if crc32.Checksum(dataBuf, common.CastagnoliCrcTable) !=
			binary.BigEndian.Uint32(crcBuf[4:8]) {
			return &Manifest{}, 0, common.ErrBadChecksum
		}
		var changeSet pb.ManifestChangeSet
		if err := changeSet.Unmarshal(dataBuf); err != nil {
			return &Manifest{}, 0, err
		}
		if err = applyChangeSet(manifest, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
		offset += readCrcNum + readDataNum
	}
	return manifest, int64(offset), nil
}

func applyChangeSet(manifest *Manifest, changeSet *pb.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyManifestChange(manifest, change); err != nil {
			return err
		}
	}
	return nil
}

func applyManifestChange(manifest *Manifest, change *pb.ManifestChange) error {
	switch change.Type {
	case pb.ManifestChange_Create:
		if _, ok := manifest.Tables[change.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", change.Id)
		}
		manifest.Tables[change.Id] = TableManifest{
			ID:       change.Id,
			LevelID:  uint8(change.LevelId),
			CheckSum: append([]byte{}, change.CheckSum...),
		}
		for len(manifest.Levels) <= int(change.LevelId) {
			manifest.Levels = append(manifest.Levels, LevelManifest{TableIds: make(map[uint64]struct{}, 0)})
		}
		manifest.Levels[change.LevelId].TableIds[change.Id] = struct{}{}
		manifest.Creations++
	case pb.ManifestChange_Delete:
		tm, ok := manifest.Tables[change.Id]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d", change.Id)
		}
		delete(manifest.Tables, change.Id)
		delete(manifest.Levels[tm.LevelID].TableIds, change.Id)
		manifest.Deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange op")
	}
	return nil
}

func NewManifest() *Manifest {
	levels := make([]LevelManifest, 0)
	tables := make(map[uint64]TableManifest, 0)
	return &Manifest{
		Levels:    levels,
		Tables:    tables,
		Creations: 0,
		Deletions: 0,
	}
}

func (mf *ManifestFile) rewrite() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	rewrite, creations, err := doRewrite(mf.opt.Dir, mf.manifest)
	if err != nil {
		return err
	}
	mf.f = rewrite
	mf.manifest.Creations = creations
	mf.manifest.Deletions = 0
	return nil
}

func (mf *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) (err error) {
	createChange := mf.manifest.newCreateChange(t.ID, levelNum, t.Checksum)
	err = mf.addChanges([]*pb.ManifestChange{createChange})
	return err
}

func (mf *ManifestFile) AddChanges(changes []*pb.ManifestChange) error {
	return mf.addChanges(changes)
}

func (mf *ManifestFile) addChanges(changes []*pb.ManifestChange) error {
	changeSets := pb.ManifestChangeSet{Changes: changes}
	// 每次进行序列化字节数据;
	buf, err := changeSets.Marshal()
	if err != nil {
		return err
	}
	mf.mux.Lock()
	defer mf.mux.Unlock()
	// 1. 应用到内存中
	if err := applyChangeSet(mf.manifest, &changeSets); err != nil {
		return err
	}

	if mf.manifest.Deletions > common.ManifestDeletionsRewriteThreshold &&
		mf.manifest.Deletions > common.ManifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		crcBuf := make([]byte, common.ManifestFileCrcLen)
		binary.BigEndian.PutUint32(crcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(crcBuf[4:8], crc32.Checksum(buf, common.CastagnoliCrcTable))
		crcBuf = append(crcBuf, buf...)
		if _, err := mf.f.Write(crcBuf); err != nil {
			return err
		}
	}
	if err = mf.f.Sync(); err != nil {
		return err
	}
	return nil
}

func doRewrite(path string, manifest *Manifest) (*os.File, int, error) {
	reWriteFileName := filepath.Join(path, common.ManifestRewriteFilename)
	file, err := os.OpenFile(reWriteFileName, os.O_RDWR, 0666)
	if err != nil {
		return nil, 0, err
	}
	// HEAD:4 version:4
	headerBuf := make([]byte, common.ManifestFileHeaderLen)
	copy(headerBuf[0:4], common.MagicText[:])
	binary.BigEndian.PutUint32(headerBuf[4:8], common.MagicVersion)

	creations := len(manifest.Tables)
	asChanges := manifest.asChanges()
	changeSet := pb.ManifestChangeSet{Changes: asChanges}
	changeBuf, err := changeSet.Marshal()
	if err != nil {
		file.Close()
		return nil, 0, err
	}
	crcBuf := make([]byte, common.ManifestFileCrcLen)
	binary.BigEndian.PutUint32(crcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(crcBuf[4:8], crc32.Checksum(changeBuf, common.CastagnoliCrcTable))
	headerBuf = append(headerBuf, crcBuf...)
	headerBuf = append(headerBuf, changeBuf...)
	if _, err := file.Write(headerBuf); err != nil {
		file.Close()
		return nil, 0, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, 0, err
	}
	if err = file.Close(); err != nil {
		return nil, 0, err
	}

	manifestPathName := filepath.Join(path, common.ManifestFilename)
	if err = os.Rename(reWriteFileName, manifestPathName); err != nil {
		return nil, 0, err
	}
	openFile, err := os.OpenFile(manifestPathName, common.DefaultFileFlag, common.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}
	_, err = openFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, 0, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, 0, err
	}
	return file, creations, nil
}
func (m *Manifest) asChanges() []*pb.ManifestChange {
	manifestChanges := make([]*pb.ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		manifestChanges = append(manifestChanges, m.newCreateChange(id, int(tm.LevelID), tm.CheckSum))
	}
	return manifestChanges
}

func (m *Manifest) newCreateChange(id uint64, level int, checkSum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Type:     pb.ManifestChange_Create,
		LevelId:  uint32(level),
		CheckSum: checkSum,
	}
}

func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.manifest
}

func (mf *ManifestFile) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

func (mf *ManifestFile) checkSSTable(ids map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	for _, table := range mf.manifest.Tables {
		if _, ok := ids[table.ID]; !ok {
			return fmt.Errorf("sst file  can`t does not exist for table %d", table.ID)
		}
	}
	// 2. Delete files that shouldn't exist.
	for id := range ids {
		if _, ok := mf.manifest.Tables[id]; !ok {
			common.Err(fmt.Errorf("Table file %d  not referenced in MANIFEST", id))
			ssTablePath := GetSSTablePathFromId(mf.opt.Dir, id)
			if err := os.Remove(ssTablePath); err != nil {
				return common.ErrBadRemoveSST
			}
		}
	}
	return nil
}
