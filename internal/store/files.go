package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

func (s *Store) fileKey(id string) string     { return s.key("file", id) }
func (s *Store) fileBodyKey(id string) string { return s.key("filebody", id) }

// PutFile stores metadata and body. The id must be unique.
func (s *Store) PutFile(ctx context.Context, f *model.File, body []byte) error {
	raw, err := mustJSON(f)
	if err != nil {
		return err
	}
	ok, err := s.rdb.SetNX(ctx, s.fileKey(f.ID), raw, 0).Result()
	if err != nil {
		return fmt.Errorf("put file meta: %w", err)
	}
	if !ok {
		return fmt.Errorf("file %s already exists", f.ID)
	}
	if err := s.rdb.Set(ctx, s.fileBodyKey(f.ID), body, 0).Err(); err != nil {
		return fmt.Errorf("put file body: %w", err)
	}
	return nil
}

// GetFile loads file metadata.
func (s *Store) GetFile(ctx context.Context, id string) (*model.File, error) {
	var f model.File
	if err := s.getJSON(ctx, s.fileKey(id), &f); err != nil {
		return nil, err
	}
	return &f, nil
}

// GetFileContent loads the raw bytes of a file.
func (s *Store) GetFileContent(ctx context.Context, id string) ([]byte, error) {
	b, err := s.rdb.Get(ctx, s.fileBodyKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get file body: %w", err)
	}
	return b, nil
}

// DeleteFile removes metadata and body.
func (s *Store) DeleteFile(ctx context.Context, id string) error {
	n, err := s.rdb.Del(ctx, s.fileKey(id), s.fileBodyKey(id)).Result()
	if err != nil {
		return fmt.Errorf("delete file: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
